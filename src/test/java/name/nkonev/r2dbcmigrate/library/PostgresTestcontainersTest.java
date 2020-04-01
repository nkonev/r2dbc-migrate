package name.nkonev.r2dbcmigrate.library;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Mono;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;

public class PostgresTestcontainersTest {
    final static int POSTGRESQL_PORT = 5432;
    static GenericContainer container;

    static Logger fooLogger;
    static Level previousLevel;

    @BeforeAll
    public static void beforeAll() {
        container = new GenericContainer("postgres:12.2")
                .withExposedPorts(POSTGRESQL_PORT)
                .withEnv("POSTGRES_PASSWORD", "postgresqlPassword")
                .withClasspathResourceMapping("docker/postgresql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                        .withTimes(2));
        container.start();

        fooLogger = (Logger) LoggerFactory.getLogger("io.r2dbc.postgresql.QUERY");
        previousLevel = fooLogger.getEffectiveLevel();
        fooLogger.setLevel(Level.DEBUG); // TODO here I override maven logger
    }

    @AfterAll
    public static void afterAll() {
        container.stop();
        fooLogger.setLevel(previousLevel);
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
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        // add the appender to the logger
        // addAppender is outdated now
        fooLogger.addAppender(listAppender);

        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPath("file:./migrations/postgresql/*.sql");

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).blockLast();

        // get log
        List<ILoggingEvent> logsList = listAppender.list;
        listAppender.stop();
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(Collectors.toList());
        Assertions.assertTrue(
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
        // make asserts
    }

}
