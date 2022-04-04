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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import name.nkonev.r2dbc.migrate.core.FilenameParser.MigrationInfo;
import name.nkonev.r2dbc.migrate.reader.SpringResourceReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static name.nkonev.r2dbc.migrate.core.ListUtils.hasSubList;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MssqlTestcontainersTest extends LogCaptureableTests {

    final static int MSSQL_PORT = 1433;

    static GenericContainer container;
    final static String password = "yourStrong(!)Password";

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @BeforeEach
    public void beforeEach()  {
        container = new GenericContainer("mcr.microsoft.com/mssql/server:2017-CU22-ubuntu-16.04")
                .withExposedPorts(MSSQL_PORT)
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", password)
                .withEnv("MSSQL_COLLATION", "cyrillic_general_ci_as")
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*The default collation was successfully changed.*\\s")
                        .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));

        container.start();

        statementsLogger = (Logger) LoggerFactory.getLogger("io.r2dbc.mssql.QUERY");
        statementsPreviousLevel = statementsLogger.getEffectiveLevel();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
        statementsLogger.setLevel(statementsPreviousLevel);
    }

    private ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mssql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, "sa")
                .option(PASSWORD, password)
                .option(DATABASE, "master")
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

    static class Client {
        String firstName, secondName, account;
        int estimatedMoney;

        public Client(String firstName, String secondName, String account, int estimatedMoney) {
            this.firstName = firstName;
            this.secondName = secondName;
            this.account = account;
            this.estimatedMoney = estimatedMoney;
        }
    }

    private static SpringResourceReader springResourceReader = new SpringResourceReader();


    @Test
    public void testDefaultValidationResult() {
        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql/*.sql"));

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();

        Flux<Client> clientFlux = Flux.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Client(
                        row.get("first_name", String.class),
                        row.get("second_name", String.class),
                        row.get("account", String.class),
                        row.get("estimated_money", Integer.class)
                    );
                })),
            Connection::close);

        Client client = clientFlux.blockLast();

        Assertions.assertEquals("John", client.firstName);
        Assertions.assertEquals("Smith", client.secondName);
        Assertions.assertEquals("4444", client.account);
        Assertions.assertEquals(9999999, client.estimatedMoney);
    }

    @Test
    public void testCreateMsSqlDatabaseThenSchemaInItThenTableInIt() {
        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql/*.sql"));
        properties.setValidationQuery("SELECT collation_name as result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();

        Flux<Client> clientFlux = Flux.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Client(
                        row.get("first_name", String.class),
                        row.get("second_name", String.class),
                        row.get("account", String.class),
                        row.get("estimated_money", Integer.class)
                    );
                })),
            Connection::close);

        Client client = clientFlux.blockLast();

        Assertions.assertEquals("John", client.firstName);
        Assertions.assertEquals("Smith", client.secondName);
        Assertions.assertEquals("4444", client.account);
        Assertions.assertEquals(9999999, client.estimatedMoney);
    }

    @Test
    public void testCreateMsSqlDatabaseThenSchemaInItThenTableInItWithSchema() {
        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql/*.sql"));
        properties.setValidationQuery("SELECT collation_name as result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");
        properties.setMigrationsSchema("my scheme");
        properties.setMigrationsTable("my migrations");
        properties.setMigrationsLockTable("my migrations lock");

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

        Flux<Client> clientFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Client(
                        row.get("first_name", String.class),
                        row.get("second_name", String.class),
                        row.get("account", String.class),
                        row.get("estimated_money", Integer.class)
                    );
                })),
            Connection::close);

        Client client = clientFlux.blockLast();

        Assertions.assertEquals("John", client.firstName);
        Assertions.assertEquals("Smith", client.secondName);
        Assertions.assertEquals("4444", client.account);
        Assertions.assertEquals(9999999, client.estimatedMoney);


        Flux<MigrationInfo> miFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from \"my scheme\".\"my migrations\" order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new MigrationInfo(
                        String.valueOf(row.get("id", Integer.class)),
                        row.get("description", String.class),
                        false,
                        false
                    );
                })),
            Connection::close
        );
        List<MigrationInfo> migrationInfos = miFlux.collectList().block();
        Assertions.assertFalse(migrationInfos.isEmpty());
        Assertions.assertEquals("my db", migrationInfos.get(0).getDescription());

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
    public void testAppendCreateMsSqlDatabase() {
        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql/*.sql"));
        properties.setValidationQuery("SELECT collation_name as result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();

        // here we simulate new launch
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql_append/*.sql"));
        // and we assert that we able to add yet another database (nontransactional should work)
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
    }

    @Test
    public void testThatLockIsReleasedAfterError() {
        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = startAppender();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mssql_error/*.sql"));

        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        RuntimeException thrown = Assertions.assertThrows(
            RuntimeException.class,
            () -> {
                R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
            },
            "Expected exception to throw, but it didn't"
        );
        Assertions.assertTrue(thrown.getMessage().contains("Incorrect syntax near 'schyachne'."));

        // get log
        List<ILoggingEvent> logsList = stopAppenderAndGetLogsList(listAppender);
        List<Object> collect = logsList.stream().map(iLoggingEvent -> iLoggingEvent.getArgumentArray()[0]).collect(
            Collectors.toList());
        // make asserts
        assertTrue(
            hasSubList(collect, Arrays.asList(
                "update \"migrations_lock\" set locked = 'false' where id = 1"
            )));

        Mono<Boolean> r = Mono.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Mono.from(connection.createStatement("select locked from migrations_lock where id = 1").execute())
                            .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Boolean.class, null)))),
            Connection::close);
        Boolean block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertFalse(block);
    }

}