package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.*;
import java.util.ArrayList;
import java.util.logging.Level;
import name.nkonev.r2dbc.migrate.core.FilenameParser.MigrationInfo;
import name.nkonev.r2dbc.migrate.reader.MigrateResource;
import name.nkonev.r2dbc.migrate.reader.MigrateResourceReader;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import reactor.util.retry.Retry;

import static java.util.Optional.ofNullable;

public abstract class R2dbcMigrate {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrate.class);
    private static final String ROWS_UPDATED = "By '{}' {} rows updated";

    private static List<MigrateResource> getResources(String resourcesPath, MigrateResourceReader resourceReader) {
        return resourceReader.getResources(resourcesPath);
    }

    private static SqlQueries getSqlQueries(R2dbcMigrateProperties properties, Connection connection) {
        if (properties.getDialect() == null) {
            Optional<String> maybeDb = ofNullable(connection.getMetadata())
                .map(md -> md.getDatabaseProductName())
                .map(s -> s.toLowerCase());
            if (maybeDb.isPresent()) {
                if (maybeDb.get().contains("postgres")) {
                    return new PostgreSqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                } else if (maybeDb.get().contains("microsoft")) {
                    return new MSSqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                } else if (maybeDb.get().contains("mysql")) {
                    return new MySqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                } else if (maybeDb.get().contains("h2")) {
                    return new H2Queries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                }
            }
            throw new RuntimeException("Cannot recognize dialect. Try to set it explicitly.");
        } else {
            switch (properties.getDialect()) {
                case POSTGRESQL:
                    return new PostgreSqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                case MSSQL:
                    return new MSSqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                case MYSQL:
                    return new MySqlQueries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                case H2:
                    return new H2Queries(properties.getMigrationsSchema(), properties.getMigrationsTable(), properties.getMigrationsLockTable());
                default:
                    throw new RuntimeException("Unsupported dialect: " + properties.getDialect());
            }
        }
    }

    private static <T> Flux<T> withAutoCommit(Connection connection, Publisher<T> action) {
        return Mono.from(connection.setAutoCommit(true)).thenMany(action);
    }

    private static Mono<Void> transactionalWrap(Connection connection, boolean transactional,
        Publisher<? extends io.r2dbc.spi.Result> migrationThings, String info) {

        Mono<Integer> integerFlux = Flux.from(migrationThings)
                .flatMap(Result::getRowsUpdated) // if we don't get rows updates we swallow potential errors from PostgreSQL
                .switchIfEmpty(Mono.just(0)) // prevent emitting empty flux
                .reduceWith(() -> 0, Integer::sum)
                .doOnSuccess(integer -> {
                    LOGGER.info(ROWS_UPDATED, info, integer);
                });

        Mono<Void> result;
        if (transactional) {
            result = Mono.from(connection.beginTransaction()) // 1
                    .thenMany(integerFlux) // 2 create internals
                    .then(Mono.from(connection.commitTransaction())); // 3
        } else {
            result = withAutoCommit(connection, integerFlux).then();
        }
        return result;
    }

    private static <T> Mono<Void> transactionalWrapUnchecked(Connection connection,
        boolean transactional, Publisher<T> migrationThings) {

        Flux<T> integerFlux = Flux.from(migrationThings);

        Mono<Void> result;
        if (transactional) {
            result = Mono.from(connection.beginTransaction()) // 1
                    .thenMany(integerFlux) // 2 create internals
                    .then(Mono.from(connection.commitTransaction())); // 3
        } else {
            result = withAutoCommit(connection, integerFlux).then();
        }
        return result;
    }

    private static Mono<Void> waitForDatabase(ConnectionFactory connectionFactory, R2dbcMigrateProperties properties) {
        if (!properties.isWaitForDatabase()) {
            return Mono.empty();
        }

        Mono<String> testResult = Mono.usingWhen(Mono.defer(() -> {
                LOGGER.info("Creating new test connection");
                return Mono.from(connectionFactory.create());
            }),
            connection -> Flux
                .from(connection.validate(ValidationDepth.REMOTE))
                .filter(Boolean.TRUE::equals)
                .switchIfEmpty(Mono.error(new RuntimeException("Connection is not valid")))
                .then(
                    Flux.from(connection.createStatement(properties.getValidationQuery()).execute())
                        .flatMap(o -> o.map(getResultSafely("result", String.class,
                                "__VALIDATION_RESULT_NOT_PROVIDED")))
                        .filter(s -> {
                            LOGGER.info("Comparing expected value '{}' with provided result '{}'",
                                properties.getValidationQueryExpectedResultValue(), s);
                            return properties.getValidationQueryExpectedResultValue().equals(s);
                        })
                        .switchIfEmpty(Mono.error(new RuntimeException("Not matched result of test query")))
                        .last()
                ),
            connection -> {
                LOGGER.info("Closing test connection");
                return connection.close();
            }).log("R2dbcMigrateCreatingTestConnection", Level.FINE);

        return testResult
            .timeout(properties.getValidationQueryTimeout())
            .retryWhen(Retry.fixedDelay(properties.getConnectionMaxRetries(),
            properties.getValidationRetryDelay()).doBeforeRetry(retrySignal -> {
                LOGGER.warn("Retrying to get database connection due {}: {}",
                    retrySignal.failure().getClass(),
                    retrySignal.failure().getMessage());
            }))
            .doOnSuccess(o -> LOGGER.info("Successfully got result '{}' of test query", o))
            .then();
    }

    // entrypoint
    public static Mono<Void> migrate(ConnectionFactory connectionFactory, R2dbcMigrateProperties properties, MigrateResourceReader resourceReader, SqlQueries maybeUserDialect) {
        LOGGER.info("Configured with {}", properties);
        if (!properties.isEnable()) {
            return Mono.empty();
        }
        return waitForDatabase(connectionFactory, properties)
            // here we opens new connection and make all migration stuff
            .then(Mono.usingWhen(
                connectionFactory.create(),
                connection -> doWork(connection, properties, resourceReader, maybeUserDialect),
                Connection::close
            ));
    }

    private static Mono<Void> ensureInternals(Connection connection, SqlQueries sqlQueries) {
        Batch createInternals = connection.createBatch();
        sqlQueries.createInternalTables().forEach(createInternals::add);
        Publisher<? extends Result> createInternalTables = createInternals.execute();
        return transactionalWrap(connection, true, createInternalTables, "Making internal tables");
    }

    private static Mono<Void> acquireOrWaitForLock(Connection connection, SqlQueries sqlQueries, R2dbcMigrateProperties properties) {
        Mono<Integer> lockUpdated = Mono.from(connection.createStatement(sqlQueries.tryAcquireLock()).execute())
                .flatMap(o -> Mono.from(o.getRowsUpdated()))
                .switchIfEmpty(Mono.just(0))
                .flatMap(integer -> {
                    if (Integer.valueOf(0).equals(integer)) {
                        return Mono.error(new RuntimeException("Equals zero"));
                    } else {
                        return Mono.just(integer);
                    }
                })
                .doOnSuccess(integer -> {
                    LOGGER.info(ROWS_UPDATED, "Acquiring lock", integer);
                });

        Mono<Integer> waitForLock = lockUpdated.retryWhen(reactor.util.retry.Retry.fixedDelay(properties.getAcquireLockMaxRetries(), properties.getAcquireLockRetryDelay()).doAfterRetry(retrySignal -> {
            LOGGER.warn("Waiting for lock");
        }));
        return transactionalWrapUnchecked(connection, true, waitForLock);
    }

    private static List<Tuple2<MigrateResource, FilenameParser.MigrationInfo>> getResourcesFromPath(String resourcesPath, MigrateResourceReader resourceReader) {
        List<Tuple2<MigrateResource, FilenameParser.MigrationInfo>> collect = getResources(resourcesPath, resourceReader).stream()
            .filter(Objects::nonNull)
            .filter(MigrateResource::isReadable)
            .map(resource -> {
                LOGGER.debug("Reading {}", resource);
                FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(resource.getFilename());
                return Tuples.of(resource, migrationInfo);
            })
            .collect(Collectors.toList());
        return collect;
    }

    private static Flux<Tuple2<MigrateResource, FilenameParser.MigrationInfo>> getFileResources(R2dbcMigrateProperties properties, MigrateResourceReader resourceReader) {
        List<Tuple2<MigrateResource, FilenameParser.MigrationInfo>> allResources = new ArrayList<>();
        for (String resource: properties.getResourcesPaths()) {
            allResources.addAll(getResourcesFromPath(resource, resourceReader));
        }
        List<Tuple2<MigrateResource, MigrationInfo>> sortedResources = allResources.stream().sorted((o1, o2) -> {
            MigrationInfo migrationInfo1 = o1.getT2();
            MigrationInfo migrationInvo2 = o2.getT2();
            return Integer.compare(migrationInfo1.getVersion(), migrationInvo2.getVersion());
        }).peek(objects -> {
            LOGGER.debug("From {} parsed metadata {}", objects.getT1(), objects.getT2());
        }).collect(Collectors.toList());
        return Flux.fromIterable(sortedResources);
    }

    private static Mono<Void> releaseLock(Connection connection, SqlQueries sqlQueries) {
        return transactionalWrap(connection, true, (connection.createStatement(sqlQueries.releaseLock()).execute()), "Releasing lock");
    }

    private static Mono<Void> releaseLockAfterError(Throwable throwable, Connection connection, SqlQueries sqlQueries) {
        LOGGER.error("Got error", throwable);
        return transactionalWrap(connection, false, (connection.createStatement(sqlQueries.releaseLock()).execute()), "Releasing lock after error");
    }

    private static Mono<Void> doWork(Connection connection, R2dbcMigrateProperties properties, MigrateResourceReader resourceReader, SqlQueries maybeUserDialect) {
        SqlQueries sqlQueries = maybeUserDialect == null ? getSqlQueries(properties, connection) : maybeUserDialect;
        LOGGER.debug("Instantiated {}", sqlQueries.getClass());

        return
                ensureInternals(connection, sqlQueries)
                        .log("R2dbcMigrateEnsuringInternals", Level.FINE)
                        .then(acquireOrWaitForLock(connection, sqlQueries, properties).log("R2dbcMigrateAcquiringLock", Level.FINE))
                        .then(getDatabaseVersionOrZero(sqlQueries, connection, properties).log("R2dbcMigrateGetDatabaseVersion", Level.FINE))
                        .flatMap(currentVersion -> {
                            LOGGER.info("Database version is {}", currentVersion);

                            return getFileResources(properties, resourceReader).log("R2dbcMigrateRequestingMigrationFiles", Level.FINE)
                                    .filter(objects -> objects.getT2().getVersion() > currentVersion)
                                    // We need to guarantee sequential queries for BEGIN; STATEMENTS; COMMIT; wrappings for PostgreSQL
                                    .concatMap(tuple2 ->
                                            makeMigration(connection, properties, tuple2).log("R2dbcMigrateMakeMigrationWork", Level.FINE)
                                                .then(writeMigrationMetadata(connection, sqlQueries, tuple2).log("R2dbcMigrateWritingMigrationMetadata", Level.FINE))
                                    , 1)
                                    .onErrorResume(throwable -> releaseLockAfterError(throwable, connection, sqlQueries).then(Mono.error(throwable)))
                                    .then(releaseLock(connection, sqlQueries).log("R2dbcMigrateReleasingLock", Level.FINE));
                        });

    }

    private static Mono<Void> makeMigration(Connection connection, R2dbcMigrateProperties properties, Tuple2<MigrateResource, FilenameParser.MigrationInfo> tt) {
        LOGGER.info("Applying {}", tt.getT2());
        return transactionalWrap(connection, tt.getT2().isTransactional(), getMigrateResultPublisher(properties, connection, tt.getT1(), tt.getT2()), tt.getT2().toString());
    }

    private static Mono<Void> writeMigrationMetadata(Connection connection, SqlQueries sqlQueries, Tuple2<MigrateResource, FilenameParser.MigrationInfo> tt) {
        return transactionalWrap(connection, true, sqlQueries.createInsertMigrationStatement(connection, tt.getT2()).execute(), "Writing metadata version " + tt.getT2().getVersion());
    }

    private static Mono<Integer> getDatabaseVersionOrZero(SqlQueries sqlQueries, Connection connection, R2dbcMigrateProperties properties) {

        return withAutoCommit(connection, connection.createStatement(sqlQueries.getMaxMigration()).execute())
            .last()
            .flatMap(o -> Mono.from(o.map(getResultSafely("max", Integer.class, 0))))
            .switchIfEmpty(Mono.just(0));
    }

    static <ColumnType> BiFunction<Row, RowMetadata, ColumnType> getResultSafely(String resultColumn, Class<ColumnType> ct, ColumnType defaultValue) {
        return (row, rowMetadata) -> {
            if (rowMetadata.getColumnNames().contains(resultColumn)) { // mssql check
                ColumnType value = row.get(resultColumn, ct);
                return value != null ? value : defaultValue;
            } else {
                return defaultValue;
            }
        };
    }

    static Batch makeBatch(Connection connection, List<String> strings) {
        Batch batch = connection.createBatch();
        strings.forEach(batch::add);
        return batch;
    }

    private static Publisher<? extends Result> getMigrateResultPublisher(R2dbcMigrateProperties properties,
                                                                         Connection connection, MigrateResource resource,
                                                                         FilenameParser.MigrationInfo migrationInfo) {
        if (migrationInfo.isSplitByLine()) {
            Flux<? extends Result> sequentFlux = FileReader
                    .readChunked(resource, properties.getFileCharset())
                    .buffer(properties.getChunkSize())
                    .concatMap(strings -> {
                        LOGGER.debug("Creating batch - for {} processing {} strings", migrationInfo, strings.size());
                        return makeBatch(connection, strings).execute();
                    }, 1);
            return sequentFlux;
        } else {
            return connection.createStatement(FileReader.read(resource, properties.getFileCharset())).execute();
        }
    }

}
