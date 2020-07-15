package name.nkonev.r2dbc.migrate.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import name.nkonev.r2dbc.migrate.core.MssqlTestcontainersTest.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static name.nkonev.r2dbc.migrate.core.ListUtils.hasSubList;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MysqlTestcontainersTest extends LogCaptureableTests {

    final static int MYSQL_PORT = 3306;

    static GenericContainer container;

    final static String user = "mysql-user";
    final static String password = "mysql-password";

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @BeforeEach
    public void beforeEach()  {
        container = new GenericContainer("mysql:5.7")
                .withClasspathResourceMapping("/docker/mysql/etc/mysql/conf.d", "/etc/mysql/conf.d", BindMode.READ_ONLY)
                .withClasspathResourceMapping("/docker/mysql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .withEnv("MYSQL_ALLOW_EMPTY_PASSWORD", "true")
                .withExposedPorts(MYSQL_PORT)
                .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds));

        container.start();

        statementsLogger = (Logger) LoggerFactory.getLogger("name.nkonev.r2dbc.migrate.core.R2dbcMigrate");
        statementsPreviousLevel = statementsLogger.getEffectiveLevel();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
        statementsLogger.setLevel(statementsPreviousLevel);
    }

    private ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, user)
                .option(PASSWORD, password)
                .option(DATABASE, "r2dbc")
                .build());
        return connectionFactory;
    }

    @Override
    protected Level getStatementsPreviousLevel() {
        return statementsPreviousLevel;
    }

    @Override
    protected Logger getStatementsLogger() {
        return statementsLogger;
    }

    static class Customer {
        Long id;
        String firstName, lastName;

        public Customer(Long id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }


    @Test
    public void testMigrationWorks() {
        Integer mappedPort = container.getMappedPort(MYSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MYSQL);
        properties.setResourcesPath("classpath:/migrations/mysql/*.sql");
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties).block();

        Flux<Customer> clientFlux = Flux.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Flux.from(connection.createStatement("select * from customer order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                  return new Customer(
                      row.get("id", Long.class),
                      row.get("first_name", String.class),
                      row.get("last_name", String.class)
                  );
                })),
            Connection::close);

        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.lastName);
        Assertions.assertEquals(6, client.id);
    }

    @Test
    public void testThatLockIsReleasedAfterError() {
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = startAppender();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MYSQL);
        properties.setResourcesPath("classpath:/migrations/mysql_error/*.sql");

        Integer mappedPort = container.getMappedPort(MYSQL_PORT);

        RuntimeException thrown = Assertions.assertThrows(
            RuntimeException.class,
            () -> {
                R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties).block();
            },
            "Expected exception to throw, but it didn't"
        );
        Assertions.assertTrue(thrown.getMessage().contains("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'haha\n"));

        // get log
        List<ILoggingEvent> logsList = stopAppenderAndGetLogsList(listAppender);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()!=null && iLoggingEvent.getArgumentArray().length > 0 ? iLoggingEvent.getArgumentArray()[0] : "_dummy").collect(
            Collectors.toList());
        // make asserts
        assertTrue(
            hasSubList(collect, Arrays.asList(
                "Releasing lock after error"
            )));

        Mono<Byte> r = Mono.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Mono.from(connection.createStatement("select locked from migrations_lock where id = 1").execute())
                .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Byte.class, null)))),
            Connection::close);
        Byte block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertEquals((byte)0, block);
    }

}
