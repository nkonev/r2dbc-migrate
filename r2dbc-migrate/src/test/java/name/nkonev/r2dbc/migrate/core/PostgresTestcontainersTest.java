package name.nkonev.r2dbc.migrate.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresTestcontainersTest {
    final static int POSTGRESQL_PORT = 5432;
    static GenericContainer container;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PostgresTestcontainersTest.class);

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @BeforeEach
    public void beforeAll() throws IOException {
        FileUtils.copyFileToDirectory(
                new File("../docker/postgresql/docker-entrypoint-initdb.d/init-r2dbc-db.sql"),
                new File("./target/test-classes/docker/postgresql/docker-entrypoint-initdb.d")
        );

        container = new GenericContainer("postgres:12.2")
                .withExposedPorts(POSTGRESQL_PORT)
                .withEnv("POSTGRES_PASSWORD", "postgresqlPassword")
                .withClasspathResourceMapping("docker/postgresql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                        .withTimes(2));
        container.start();

        statementsLogger = (Logger) LoggerFactory.getLogger("io.r2dbc.postgresql.QUERY");
        statementsPreviousLevel = statementsLogger.getEffectiveLevel();
    }

    @AfterEach
    public void afterAll() {
        container.stop();
        statementsLogger.setLevel(statementsPreviousLevel);
    }

    private Mono<Connection> makeConnectionMono(String host, int port, String database) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, host)
                .option(PORT, port)
                .option(USER, "r2dbc")
                .option(PASSWORD, "r2dbcPazZw0rd")
                .option(DATABASE, database)
                .build());
        Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
        return Mono.from(connectionPublisher);
    }

    @Test
    public void testThatTransactionsWrapsQueriesAndTransactionsAreNotNested() {
        statementsLogger.setLevel(Level.DEBUG); // TODO here I override maven logger

        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        // add the appender to the logger
        // addAppender is outdated now
        statementsLogger.addAppender(listAppender);

        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPath("classpath:/migrations/postgresql/*.sql");

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).block();

        // get log
        List<ILoggingEvent> logsList = listAppender.list;
        listAppender.stop();
        statementsLogger.setLevel(statementsPreviousLevel);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(Collectors.toList());
        // make asserts
        assertTrue(
                Collections.indexOfSubList(collect, Arrays.asList(
                        "BEGIN",
                        "create table if not exists migrations (id int primary key, description text); create table if not exists migrations_lock (id int primary key, locked boolean not null); insert into migrations_lock(id, locked) values (1, false) on conflict (id) do nothing",
                        "COMMIT",
                        "BEGIN",
                        "update migrations_lock set locked = true where id = 1 and locked = false",
                        "COMMIT",
                        "select max(id) from migrations",
                        "BEGIN",
                        "CREATE TABLE customer (id SERIAL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))",
                        "COMMIT",
                        "BEGIN",
                        "insert into migrations(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "insert into customer(first_name, last_name) values ('Muhammad', 'Ali'), ('Name', 'Фамилия');",
                        "COMMIT",
                        "BEGIN",
                        "insert into migrations(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "insert into customer(first_name, last_name) values ('Customer', 'Surname 1');; insert into customer(first_name, last_name) values ('Customer', 'Surname 2');; insert into customer(first_name, last_name) values ('Customer', 'Surname 3');; insert into customer(first_name, last_name) values ('Customer', 'Surname 4');",
                        "COMMIT",
                        "BEGIN",
                        "insert into migrations(id, description) values ($1, $2)",
                        "COMMIT",
                        "BEGIN",
                        "update migrations_lock set locked = false where id = 1",
                        "COMMIT"
                )) != -1);
    }

    @Test
    public void testValidationResultOk() {
        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setValidationQuery("select 'super value' as result");
        properties.setValidationQueryExpectedValue("super value");
        properties.setConnectionMaxRetries(1);
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPath("classpath:/migrations/postgresql/*.sql");

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).block();
    }

    @Test
    public void testValidationResultFail() {
        RuntimeException thrown = Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
                    properties.setValidationQuery("select 'not super value' as result");
                    properties.setValidationQueryExpectedValue("super value");
                    properties.setConnectionMaxRetries(1);
                    properties.setDialect(Dialect.POSTGRESQL);
                    properties.setResourcesPath("classpath:/migrations/postgresql/*.sql");

                    Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
                    R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).block();
                },
                "Expected exception to throw, but it didn't"
        );

        assertTrue(thrown.getMessage().contains("Not result of test query"));
    }

//    @Disabled
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

        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPath("file:./target/test-classes/oom_migrations/*.sql");

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).block();

    }


}
