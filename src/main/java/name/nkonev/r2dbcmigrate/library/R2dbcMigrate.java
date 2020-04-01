package name.nkonev.r2dbcmigrate.library;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Backoff;
import reactor.retry.Retry;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class R2dbcMigrate {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrate.class);
    private static final String ROWS_UPDATED = "By '{}' {} rows updated";

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
        private Charset fileCharset = StandardCharsets.UTF_8;

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

        public Charset getFileCharset() {
            return fileCharset;
        }

        public void setFileCharset(Charset fileCharset) {
            this.fileCharset = fileCharset;
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
                    ", fileCharset=" + fileCharset +
                    '}';
        }
    }

    private static List<Resource> getResources(String resourcesPath) {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources;
        try {
            resources = resolver.getResources(resourcesPath);
        } catch (IOException e) {
            throw new RuntimeException("Error during get resources from '" + resourcesPath + "'", e);
        }
        return Arrays.asList(resources);
    }

    protected static SqlQueries getSqlQueries(MigrateProperties properties) {
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

    private static Mono<Void> transactionalWrap(Connection connection, boolean transactional, Publisher<? extends io.r2dbc.spi.Result> migrationThings, String info) {
        Mono<Integer> integerFlux = Flux.from(migrationThings)
                .flatMap(Result::getRowsUpdated) // if we don't get rows updates we swallow potential errors from PostgreSQL
                .switchIfEmpty(Mono.just(0)) // prevent emitting empty flux
                .reduceWith(() -> 0, Integer::sum)
                .doOnSuccess(integer -> {
                    LOGGER.info(ROWS_UPDATED, info, integer);
                });

        if (transactional) {
            return Mono.from(connection.beginTransaction()) // 1
                    .thenMany(integerFlux) // 2 create internals
                    .then(Mono.from(connection.commitTransaction())); // 3
        } else {
            return Mono.from(connection.setAutoCommit(true)).thenMany(integerFlux).then();
        }
    }

    private static <T> Mono<Void> transactionalWrapUnchecked(Connection connection, boolean transactional, Publisher<T> migrationThings) {
        Flux<T> integerFlux = Flux.from(migrationThings);
        if (transactional) {
            return Mono.from(connection.beginTransaction()) // 1
                    .thenMany(integerFlux) // 2 create internals
                    .then(Mono.from(connection.commitTransaction())); // 3
        } else {
            return Mono.from(connection.setAutoCommit(true)).thenMany(integerFlux).then();
        }
    }

    // entrypoint
    public static Flux<Void> migrate(Supplier<Mono<Connection>> connectionSupplier,
                                  MigrateProperties properties) {
        LOGGER.info("Configured with {}", properties);
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
                    .flatMapMany(connection ->
                            doWork(connection, sqlQueries, properties)
                            .doFinally((st) -> connection.close())
                            .then()
                    )
                ;
    }

    private static Mono<Void> ensureInternals(Connection connection, SqlQueries sqlQueries) {
        Batch createInternals = connection.createBatch();
        sqlQueries.createInternalTables().forEach(createInternals::add);
        Publisher<? extends Result> createInternalTables = createInternals.execute();
        return transactionalWrap(connection, true, createInternalTables, "Making internal tables");
    }

    private static Mono<Void> acquireOrWaitForLock(Connection connection, SqlQueries sqlQueries, MigrateProperties properties) {
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
        Mono<Integer> waitForLock = lockUpdated.retryWhen(Retry.anyOf(RuntimeException.class).backoff(Backoff.fixed(properties.getAcquireLockRetryDelay())).retryMax(properties.getAcquireLockMaxRetries()).doOnRetry(objectRetryContext -> {
            LOGGER.warn("Waiting for lock");
        }));
        return transactionalWrapUnchecked(connection, true, waitForLock);
    }

    private static Flux<Tuple2<Resource, FilenameParser.MigrationInfo>> getFileResources(MigrateProperties properties) {
        List<Tuple2<Resource, FilenameParser.MigrationInfo>> collect = getResources(properties.getResourcesPath()).stream()
                .filter(Objects::nonNull)
                .filter(Resource::isReadable)
                .map(resource -> {
                    LOGGER.debug("Reading {}", resource);
                    FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(resource.getFilename());
                    return Tuples.of(resource, migrationInfo);
                }).sorted((o1, o2) -> {
                    FilenameParser.MigrationInfo migrationInfo1 = o1.getT2();
                    FilenameParser.MigrationInfo migrationInvo2 = o2.getT2();
                    return Integer.compare(migrationInfo1.getVersion(), migrationInvo2.getVersion());
                }).peek(objects -> {
                    LOGGER.info("From {} parsed metadata {}", objects.getT1(), objects.getT2());
                })
                .collect(Collectors.toList());
        return Flux.fromIterable(collect);
    }

    private static Mono<Void> releaseLock(Connection connection, SqlQueries sqlQueries) {
        return transactionalWrap(connection, true, (connection.createStatement(sqlQueries.releaseLock()).execute()), "Releasing lock");
    }

    private static Mono<Void> doWork(Connection connection, SqlQueries sqlQueries, MigrateProperties properties) {
        return
                ensureInternals(connection, sqlQueries)
                        .then(acquireOrWaitForLock(connection, sqlQueries, properties))
                        .then(getDatabaseVersionOrZero(sqlQueries, connection))
                        .flatMap(currentVersion -> {
                            LOGGER.info("Database version is {}", currentVersion);

                            Mono<Void> voidMono = getFileResources(properties)
                                    .filter(objects -> objects.getT2().getVersion() > currentVersion)
                                    .collectList()
                                    .flatMap(list -> {
                                        // We need to guarantee sequential queries for BEGIN; STATEMENTS; COMMIT; wrappings for PostgreSQL
                                        // seems here we build sequent builder chain
                                        // TODO consider replace this chain with concatMap(f, 1)
                                        Mono<Void> last = Mono.empty();
                                        for (Tuple2<Resource, FilenameParser.MigrationInfo> tt : list) {
                                            last = last.then(makeMigration(connection, properties, tt));
                                            last = last.then(writeMigrationMetadata(connection, sqlQueries, tt));
                                        }
                                        last = last.then(releaseLock(connection, sqlQueries));
                                        return last;
                                    });

                            return voidMono;
                        }); // TODO consider timeout-based retry whole chain for MS SQL Server 2019

    }

    private static Mono<Void> makeMigration(Connection connection, MigrateProperties properties, Tuple2<Resource, FilenameParser.MigrationInfo> tt) {
        return transactionalWrap(connection, tt.getT2().isTransactional(), getMigrateResultPublisher(properties, connection, tt.getT1(), tt.getT2()), tt.getT2().toString());
    }

    private static Mono<Void> writeMigrationMetadata(Connection connection, SqlQueries sqlQueries, Tuple2<Resource, FilenameParser.MigrationInfo> tt) {
        return transactionalWrap(connection, true, sqlQueries.createInsertMigrationStatement(connection, tt.getT2()).execute(), "Writing metadata version "+tt.getT2().getVersion());
    }

    private static Mono<Integer> getDatabaseVersionOrZero(SqlQueries sqlQueries, Connection connection) {
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

    static Batch makeBatch(Connection connection, List<String> strings) {
        Batch batch = connection.createBatch();
        strings.forEach(batch::add);
        return batch;
    }

    private static Publisher<? extends Result> getMigrateResultPublisher(MigrateProperties properties,
                                                                         Connection connection, Resource resource,
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
