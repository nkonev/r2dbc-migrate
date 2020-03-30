package name.nkonev.r2dbcmigrate.library;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Backoff;
import reactor.retry.Repeat;
import reactor.retry.RepeatContext;
import reactor.retry.Retry;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

public class R2dbcMigrate {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrate.class);

    public static class MigrateProperties {
        private long connectionMaxRetries = 500;
        private String resourcesPath;
        private int chunkSize = 1000;
        private Dialect dialect;
        private String validationQuery = "select 1";
        private Duration validationQueryTimeout = Duration.ofSeconds(5);
        private Duration validationRetryDelay = Duration.ofSeconds(1);
        private Duration acquireLockRetryDelay = Duration.ofSeconds(1);
        private long acquireLockMaxRetries = 100;

        public MigrateProperties() {
        }

        public String getResourcesPath() {
            return resourcesPath;
        }

        public void setResourcesPath(String resourcesPath) {
            this.resourcesPath = resourcesPath;
        }

        public long getConnectionMaxRetries() {
            return connectionMaxRetries;
        }

        public void setConnectionMaxRetries(long connectionMaxRetries) {
            this.connectionMaxRetries = connectionMaxRetries;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public Dialect getDialect() {
            return dialect;
        }

        public void setDialect(Dialect dialect) {
            this.dialect = dialect;
        }

        public String getValidationQuery() {
            return validationQuery;
        }

        public void setValidationQuery(String validationQuery) {
            this.validationQuery = validationQuery;
        }

        public Duration getValidationQueryTimeout() {
            return validationQueryTimeout;
        }

        public void setValidationQueryTimeout(Duration validationQueryTimeout) {
            this.validationQueryTimeout = validationQueryTimeout;
        }

        public Duration getValidationRetryDelay() {
            return validationRetryDelay;
        }

        public void setValidationRetryDelay(Duration validationRetryDelay) {
            this.validationRetryDelay = validationRetryDelay;
        }

        public Duration getAcquireLockRetryDelay() {
            return acquireLockRetryDelay;
        }

        public void setAcquireLockRetryDelay(Duration acquireLockRetryDelay) {
            this.acquireLockRetryDelay = acquireLockRetryDelay;
        }


        public long getAcquireLockMaxRetries() {
            return acquireLockMaxRetries;
        }

        public void setAcquireLockMaxRetries(long acquireLockMaxRetries) {
            this.acquireLockMaxRetries = acquireLockMaxRetries;
        }

        @Override
        public String toString() {
            return "MigrateProperties{" +
                    "connectionMaxRetries=" + connectionMaxRetries +
                    ", resourcesPath='" + resourcesPath + '\'' +
                    ", chunkSize=" + chunkSize +
                    ", dialect=" + dialect +
                    ", validationQuery='" + validationQuery + '\'' +
                    ", validationQueryTimeout=" + validationQueryTimeout +
                    ", validationRetryDelay=" + validationRetryDelay +
                    ", acquireLockRetryDelay=" + acquireLockRetryDelay +
                    ", acquireLockMaxRetries=" + acquireLockMaxRetries +
                    '}';
        }
    }

    private static Flux<Resource> getResources(String resourcesPath) {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources;
        try {
            resources = resolver.getResources(resourcesPath);
        } catch (IOException e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resourcesPath + "'", e));
        }
        return Flux.just(resources);
    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

    private static Flux<String> fromResource(Resource resource) {
        try {
            return fromPath(resource.getFile().toPath());
        } catch (IOException e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resource + "'", e));
        }
    }

    private static String getString(Resource resource) {
        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
    }


    protected SqlQueries getSqlQueries(MigrateProperties properties) {
        if (properties.getDialect() == null) {
            throw new RuntimeException("Dialect cannot be null");
        }
        switch (properties.getDialect()) {
            case POSTGRESQL:
                return new PostgreSqlQueries();
            case MSSQL:
                return new MSSqlQueries();
            default:
                throw new RuntimeException("Unsupported dialect: " + properties.getDialect());
        }
    }

    private Mono<Void> doWork(Connection connection, SqlQueries sqlQueries, MigrateProperties properties) {
        Batch createInternals = connection.createBatch();
        sqlQueries.createInternalTables().forEach(createInternals::add);
        Publisher<? extends Result> createInternalTables = createInternals.execute();
        Flux<Tuple2<Integer, FilenameParser.MigrationInfo>> internalsCreation = addMigrationInfoToResult(getInternalTablesCreation(), createInternalTables);

        Flux<Tuple2<Resource, FilenameParser.MigrationInfo>> fileResources = getResources(properties.getResourcesPath()).map(resource -> {
            LOGGER.debug("Reading {}", resource);
            FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(resource.getFilename());
            LOGGER.info("From {} parsed metadata {}", resource, migrationInfo);
            return Tuples.of(resource, migrationInfo);
        });

        Mono<Integer> lockUpdated = Mono.from(connection.createStatement(sqlQueries.tryAcquireLock()).execute())
            .flatMap(o -> Mono.from(o.getRowsUpdated()))
            .switchIfEmpty(Mono.just(0))
            .flatMap(integer -> {
                if (Integer.valueOf(0).equals(integer)) {
                    return Mono.error(new RuntimeException("Equals zero"));
                } else {
                    return Mono.just(integer);
                }
            });
        Mono<Integer> waitForLock = lockUpdated.retryWhen(Retry.anyOf(RuntimeException.class).backoff(Backoff.fixed(properties.getAcquireLockRetryDelay())).retryMax(properties.getAcquireLockMaxRetries()).doOnRetry(objectRetryContext -> {
            LOGGER.warn("Waiting for lock");
        }));

        Mono<Integer> max = getMaxOrZero(sqlQueries, connection);
        Flux<Tuple2<Integer, FilenameParser.MigrationInfo>> rowsAffectedByMigration = max.flatMapMany(maxMigrationNumber -> {
            LOGGER.info("Database version is {}", maxMigrationNumber);
            return fileResources
                    .filter(objects -> objects.getT2().getVersion() > maxMigrationNumber)
                    .flatMap(fileResourceTuple -> {
                        Resource resource = fileResourceTuple.getT1();
                        FilenameParser.MigrationInfo migrationInfo = fileResourceTuple.getT2();

                        Publisher<? extends Result> migrateResultPublisher = getMigrateResultPublisher(properties, connection, resource, migrationInfo);
                        Flux<Tuple2<Integer, FilenameParser.MigrationInfo>> migrationResult = addMigrationInfoToResult(migrationInfo, migrateResultPublisher);

                        Publisher<? extends Result> migrationUp = sqlQueries
                                .createInsertMigrationStatement(connection, migrationInfo)
                                .execute();
                        Flux<Tuple2<Integer, FilenameParser.MigrationInfo>> updateInternalsFlux = addMigrationInfoToResult(getInternalTablesUpdate(migrationInfo), migrationUp);
                        return migrationResult.concatWith(updateInternalsFlux);
                    });
        });
        return internalsCreation
                .thenMany(waitForLock)
                .thenMany(rowsAffectedByMigration)
                .then(Mono.from(connection.createStatement(sqlQueries.releaseLock()).execute()))
                .then();
    }

    public Flux<Void> migrate(Supplier<Mono<Connection>> connectionSupplier,
                                  MigrateProperties properties) {
        LOGGER.info("Configuration is {}", properties);
        SqlQueries sqlQueries = getSqlQueries(properties);

        LOGGER.debug("Instantiated {}", sqlQueries.getClass());
        return
            // Here we build cold publisher which will recreate ConnectionFactory if test query fails.
            // It need for MssqlConnectionFactory. MssqlConnectionFactory becomes broken if we make requests immediately after database started.
            Mono.defer(connectionSupplier)
                    .log("Creating test connection")
                    .flatMap(testConnection -> Mono.from(testConnection.createStatement(properties.getValidationQuery()).execute()).doFinally(signalType -> testConnection.close()))
                    .timeout(properties.getValidationQueryTimeout())
                    .retryWhen(Retry.anyOf(Exception.class).backoff(Backoff.fixed(properties.getValidationRetryDelay())).retryMax(properties.getConnectionMaxRetries()).doOnRetry(objectRetryContext -> {
                        LOGGER.warn("Retrying to get database connection due {}: {}", objectRetryContext.exception().getClass(), objectRetryContext.exception().getMessage());
                    }))
                    .doOnSuccess(o -> LOGGER.info("Successfully got result of test query"))
                    // here we opens new connection and make all migration stuff
                    .then(connectionSupplier.get())
                    .log("Make migration work")
                    .flatMapMany(
                            connection -> Mono.from(connection.beginTransaction())
                                    .thenMany(connection.setAutoCommit(false))
                                    .thenMany(doWork(connection, sqlQueries, properties))
//                                    .doOnEach(tuple2Signal -> {
//                                        if (tuple2Signal.hasValue()) {
//                                            Tuple2<Integer, FilenameParser.MigrationInfo> objects = tuple2Signal.get();
//                                            if (!objects.getT2().isInternal()) {
//                                                LOGGER.info("{}: {} rows affected", objects.getT2(), objects.getT1());
//                                            } else if (LOGGER.isDebugEnabled()) {
//                                                LOGGER.debug("{}: {} rows affected", objects.getT2(), objects.getT1());
//                                            }
//                                        }
//                                    })
                                    .then(Mono.from(connection.commitTransaction()))
                                    .doFinally((st) -> connection.close())
                    );
    }

    private Mono<Integer> getMaxOrZero(SqlQueries sqlQueries, Connection connection) {
        return Mono.from(connection.createStatement(sqlQueries.getMaxMigration()).execute())
                .flatMap(o -> Mono.from(o.map((row, rowMetadata) -> {
                    if (rowMetadata.getColumnNames().contains("max")) { // mssql check
                        Integer integer = row.get("max", Integer.class);
                        return integer != null ? integer : 0;
                    } else {
                        return 0;
                    }
                }))).switchIfEmpty(Mono.just(0)).cache();
    }

    private Flux<Tuple2<Integer, FilenameParser.MigrationInfo>> addMigrationInfoToResult(
            FilenameParser.MigrationInfo migrationInfo, Publisher<? extends Result> migrateResultPublisher
    ) {
        return Flux.from(migrateResultPublisher).flatMap(r -> Flux.from(r.getRowsUpdated())
                .switchIfEmpty(Mono.just(0))
                .map(rowsUpdated -> Tuples.of(rowsUpdated, migrationInfo))
        );
    }

    private FilenameParser.MigrationInfo getInternalTablesCreation() {
        return new FilenameParser.MigrationInfo(0, "Internal tables creation", false, true);
    }

    private FilenameParser.MigrationInfo getInternalTablesUpdate(FilenameParser.MigrationInfo migrationInfo) {
        return new FilenameParser.MigrationInfo(0, "Internal tables update '" + migrationInfo.getDescription() + "'", false, true);
    }

    private Publisher<? extends Result> getMigrateResultPublisher(MigrateProperties properties,
                                                                  Connection connection, Resource resource,
                                                                  FilenameParser.MigrationInfo migrationInfo) {
        if (migrationInfo.isSplitByLine()) {
            return fromResource(resource).buffer(properties.getChunkSize()).flatMap(strings -> {
                Batch batch = connection.createBatch();
                strings.forEach(batch::add);
                return batch.execute();
            });
        } else {
            return connection.createStatement(getString(resource)).execute();
        }
    }

}
