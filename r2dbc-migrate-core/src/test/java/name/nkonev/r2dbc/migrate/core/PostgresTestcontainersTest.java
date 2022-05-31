package name.nkonev.r2dbc.migrate.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Statement;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import name.nkonev.r2dbc.migrate.core.FilenameParser.MigrationInfo;
import name.nkonev.r2dbc.migrate.reader.ReflectionsClasspathResourceReader;
import name.nkonev.r2dbc.migrate.reader.SpringResourceReader;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;

import java.time.Duration;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static name.nkonev.r2dbc.migrate.core.ListUtils.hasSubList;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresTestcontainersTest extends LogCaptureableTests {
    final static int POSTGRESQL_PORT = 5432;
    static GenericContainer container;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PostgresTestcontainersTest.class);

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @BeforeEach
    public void beforeEach()  {
        container = new GenericContainer("postgres:13.4")
                .withExposedPorts(POSTGRESQL_PORT)
                .withEnv("POSTGRES_PASSWORD", "postgresqlPassword")
                .withClasspathResourceMapping("/docker/postgresql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                        .withTimes(2).withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));
        container.start();

        statementsLogger = (Logger) LoggerFactory.getLogger("io.r2dbc.postgresql.QUERY");
        statementsPreviousLevel = statementsLogger.getEffectiveLevel();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
        statementsLogger.setLevel(statementsPreviousLevel);
    }

    private ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, "r2dbc")
                .option(PASSWORD, "r2dbcPazZw0rd")
                .option(DATABASE, "r2dbc")
                .build());
        return connectionFactory;
    }

    static class Customer {
        String firstName, secondName;
        int id;

        public Customer(String firstName, String secondName, int id) {
            this.firstName = firstName;
            this.secondName = secondName;
            this.id = id;
        }
    }

    private static SpringResourceReader springResourceReader = new SpringResourceReader();

    @Test
    public void testThatTransactionsWrapsQueriesAndTransactionsAreNotNested() {
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = startAppender();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();

        // get log
        List<ILoggingEvent> logsList = stopAppenderAndGetLogsList(listAppender);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(
            Collectors.toList());
        // make asserts
        assertTrue(
            hasSubList(collect, Arrays.asList(
                        "BEGIN",
                        "create table if not exists \"migrations\"(id int primary key, description text); create table if not exists \"migrations_lock\"(id int primary key, locked boolean not null); insert into \"migrations_lock\"(id, locked) values (1, false) on conflict (id) do nothing",
                        "COMMIT",
                        "BEGIN",
                        "update \"migrations_lock\" set locked = true where id = 1 and locked = false",
                        "COMMIT",
                        "select max(id) from \"migrations\"",
                        "BEGIN",
                        "CREATE TABLE customer (id SERIAL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))",
                        "COMMIT",
                        "BEGIN",
                        "insert into \"migrations\"(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "insert into customer(first_name, last_name) values ('Muhammad', 'Ali'), ('Name', 'Фамилия');",
                        "COMMIT",
                        "BEGIN",
                        "insert into \"migrations\"(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "insert into customer(first_name, last_name) values ('Customer', 'Surname 1');; insert into customer(first_name, last_name) values ('Customer', 'Surname 2');; insert into customer(first_name, last_name) values ('Customer', 'Surname 3');; insert into customer(first_name, last_name) values ('Customer', 'Surname 4');",
                        "COMMIT",
                        "BEGIN",
                        "insert into \"migrations\"(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "update \"migrations_lock\" set locked = false where id = 1",
                        "COMMIT"
                )));
    }

    @Test
    public void testThatLockIsReleasedAfterError() {
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = startAppender();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql_error/*.sql"));

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);

        RuntimeException thrown = Assertions.assertThrows(
            RuntimeException.class,
            () -> {
                R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
            },
            "Expected exception to throw, but it didn't"
        );
        Assertions.assertTrue(thrown.getMessage().contains("syntax error at or near \"ololo\""));

        // get log
        List<ILoggingEvent> logsList = stopAppenderAndGetLogsList(listAppender);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(Collectors.toList());
        // make asserts
        assertTrue(
            hasSubList(collect, Arrays.asList(
                "BEGIN",
                "insert into customer(first_name, last_name) values\n"
                    + "ololo\n"
                    + "('Muhammad', 'Ali'), ('Name', 'Фамилия');"
            )));

        Mono<Boolean> r = Mono.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Mono.from(connection.createStatement("select locked from \"migrations_lock\" where id = 1").execute())
                .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Boolean.class, null)))),
            Connection::close);
        Boolean block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertFalse(block);
    }

    @Test
    public void testValidationResultOk() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setValidationQuery("select 'super value' as result");
        properties.setValidationQueryExpectedResultValue("super value");
        properties.setConnectionMaxRetries(1);
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
    }

    @Test
    public void testDefaults() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));
        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
    }

    @Test
    public void testDatabaseValidationResultFail() {
        RuntimeException thrown = Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
                    properties.setValidationQuery("select 'not super value' as result");
                    properties.setValidationQueryExpectedResultValue("super value");
                    properties.setConnectionMaxRetries(1);
                    properties.setDialect(Dialect.POSTGRESQL);
                    properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));

                    Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
                    R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
                },
                "Expected exception to throw, but it didn't"
        );

        assertTrue(thrown.getMessage().contains("Retries exhausted"));
        assertTrue(thrown.getCause().getMessage().contains("Not matched result of test query"));
    }

    @EnabledIfSystemProperty(named = "enableOomTests", matches = "true")
    @Test
    public void testSplittedLargeMigrationsFitsInMemory() throws IOException {
        // _JAVA_OPTIONS: -Xmx128m
        File generatedMigrationDir = new File("./target/test-classes/oom_migrations");
        generatedMigrationDir.mkdirs();

        FileUtils.copyDirectory(new File("./src/test/resources/migrations/postgresql"), generatedMigrationDir);

        File generatedMigration = new File(generatedMigrationDir, "V20__generated__split.sql");
        if (!generatedMigration.exists()) {
            LOGGER.info("Generating large file");
            PrintWriter pw = new PrintWriter(new FileWriter(generatedMigration));
            for (int i = 0; i < 6_000_000; i++) {
                pw.println(String.format("insert into customer(first_name, last_name) values ('Generated Name %d', 'Generated Surname %d');", i, i));
            }
            pw.close();
            LOGGER.info("Generating large file completed");
        }

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPaths(Collections.singletonList("file:./target/test-classes/oom_migrations/*.sql"));

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
    }


    @Test
    public void testOtherMigrationSchema() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setMigrationsSchema("my scheme");
        properties.setMigrationsTable("my migrations");
        properties.setMigrationsLockTable("my migrations lock");
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));
        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        ConnectionFactory connectionFactory = makeConnectionMono(mappedPort);

        Mono<Integer> integerMono = Mono.usingWhen(
            connectionFactory.create(),
            connection -> Mono
                .from(connection.createStatement("create schema \"my scheme\"").execute())
                .flatMap(o -> Mono.from(o.getRowsUpdated())),
            Connection::close
        );
        integerMono.block();

        R2dbcMigrate.migrate(connectionFactory, properties, springResourceReader, null).block();

        Flux<Customer> clientFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from customer order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Customer(
                        row.get("first_name", String.class),
                        row.get("last_name", String.class),
                        row.get("id", Integer.class)
                    );
                })),
            Connection::close
        );
        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.secondName);



        Flux<MigrationInfo> miFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from \"my scheme\".\"my migrations\" order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new MigrationInfo(
                        row.get("id", Integer.class),
                        row.get("description", String.class),
                        false,
                        false,
                            false
                    );
                })),
            Connection::close
        );
        List<MigrationInfo> migrationInfos = miFlux.collectList().block();
        Assertions.assertFalse(migrationInfos.isEmpty());
        Assertions.assertEquals("create customers", migrationInfos.get(0).getDescription());

        Mono<Boolean> r = Mono.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Mono.from(connection.createStatement("select locked from \"my scheme\".\"my migrations lock\" where id = 1").execute())
                .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Boolean.class, null)))),
            Connection::close);
        Boolean block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertFalse(block);
    }

    @Test
    public void testOtherMigrationSchemaPremigration() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setMigrationsSchema("my premigrable scheme");
        properties.setMigrationsTable("my migrations");
        properties.setMigrationsLockTable("my migrations lock");
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql_premigration/*.sql"));
        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        ConnectionFactory connectionFactory = makeConnectionMono(mappedPort);

        R2dbcMigrate.migrate(connectionFactory, properties, springResourceReader, null).block();

        Flux<Customer> clientFlux = Flux.usingWhen(
                connectionFactory.create(),
                connection -> Flux.from(connection.createStatement("select * from customer order by id").execute())
                        .flatMap(o -> o.map((row, rowMetadata) -> {
                            return new Customer(
                                    row.get("first_name", String.class),
                                    row.get("last_name", String.class),
                                    row.get("id", Integer.class)
                            );
                        })),
                Connection::close
        );
        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.secondName);



        Flux<MigrationInfo> miFlux = Flux.usingWhen(
                connectionFactory.create(),
                connection -> Flux.from(connection.createStatement("select * from \"my premigrable scheme\".\"my migrations\" order by id").execute())
                        .flatMap(o -> o.map((row, rowMetadata) -> {
                            return new MigrationInfo(
                                    row.get("id", Integer.class),
                                    row.get("description", String.class),
                                    false,
                                    false,
                                    false
                            );
                        })),
                Connection::close
        );
        List<MigrationInfo> migrationInfos = miFlux.collectList().block();
        Assertions.assertFalse(migrationInfos.isEmpty());
        Assertions.assertEquals("create customers", migrationInfos.get(0).getDescription());

        Mono<Boolean> r = Mono.usingWhen(
                makeConnectionMono(mappedPort).create(),
                connection -> Mono.from(connection.createStatement("select locked from \"my premigrable scheme\".\"my migrations lock\" where id = 1").execute())
                        .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Boolean.class, null)))),
                Connection::close);
        Boolean block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertFalse(block);
    }

    @Test
    public void testWithReflections() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setResourcesPaths(Collections.singletonList("migrations/postgresql/"));
        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        ConnectionFactory connectionFactory = makeConnectionMono(mappedPort);

        ReflectionsClasspathResourceReader reflectionsClasspathResourceReader = new ReflectionsClasspathResourceReader();

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, reflectionsClasspathResourceReader, null).block();

        Flux<Customer> clientFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from customer order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Customer(
                        row.get("first_name", String.class),
                        row.get("last_name", String.class),
                        row.get("id", Integer.class)
                    );
                })),
            Connection::close
        );
        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.secondName);
    }

    @Override
    protected Level getStatementsPreviousLevel() {
        return statementsPreviousLevel;
    }

    @Override
    protected Logger getStatementsLogger() {
        return statementsLogger;
    }


    public static class SimplePostgresqlDialect implements SqlQueries {
        @Override
        public List<String> createInternalTables() {
            return Arrays.asList(
                "create table if not exists simple_migrations(id int primary key, description text)",
                "create table if not exists simple_migrations_lock(id int primary key, locked boolean not null)",
                "insert into simple_migrations_lock(id, locked) values (1, false) on conflict (id) do nothing"
            );
        }

        @Override
        public String getMaxMigration() {
            return "select max(id) from simple_migrations";
        }

        public String insertMigration() {
            return "insert into simple_migrations(id, description) values ($1, $2)";
        }

        @Override
        public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
            return connection
                .createStatement(insertMigration())
                .bind("$1", migrationInfo.getVersion())
                .bind("$2", migrationInfo.getDescription());
        }

        @Override
        public String tryAcquireLock() {
            return "update simple_migrations_lock set locked = true where id = 1 and locked = false";
        }

        @Override
        public String releaseLock() {
            return "update simple_migrations_lock set locked = false where id = 1";
        }
    }

    @Test
    public void testCustomDialect() {
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = startAppender();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/postgresql/*.sql"));

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, new SimplePostgresqlDialect()).block();

        // get log
        List<ILoggingEvent> logsList = stopAppenderAndGetLogsList(listAppender);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(
            Collectors.toList());
        // make asserts
        assertTrue(
            hasSubList(collect, Arrays.asList(
                "BEGIN",
                "create table if not exists simple_migrations(id int primary key, description text); create table if not exists simple_migrations_lock(id int primary key, locked boolean not null); insert into simple_migrations_lock(id, locked) values (1, false) on conflict (id) do nothing",
                "COMMIT",
                "BEGIN",
                "update simple_migrations_lock set locked = true where id = 1 and locked = false",
                "COMMIT",
                "select max(id) from simple_migrations",
                "BEGIN",
                "CREATE TABLE customer (id SERIAL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))",
                "COMMIT",
                "BEGIN",
                "insert into simple_migrations(id, description) values ($1, $2)",
                "COMMIT",
                "BEGIN",
                "insert into customer(first_name, last_name) values ('Muhammad', 'Ali'), ('Name', 'Фамилия');",
                "COMMIT",
                "BEGIN",
                "insert into simple_migrations(id, description) values ($1, $2)",
                "COMMIT",
                "BEGIN",
                "insert into customer(first_name, last_name) values ('Customer', 'Surname 1');; insert into customer(first_name, last_name) values ('Customer', 'Surname 2');; insert into customer(first_name, last_name) values ('Customer', 'Surname 3');; insert into customer(first_name, last_name) values ('Customer', 'Surname 4');",
                "COMMIT",
                "BEGIN",
                "insert into simple_migrations(id, description) values ($1, $2)",
                "COMMIT",
                "BEGIN",
                "update simple_migrations_lock set locked = false where id = 1",
                "COMMIT"
            )));
    }
}
