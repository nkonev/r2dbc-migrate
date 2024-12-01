package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import name.nkonev.r2dbc.migrate.reader.SpringResourceReader;
import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MssqlTestcontainersTest {

    final static int MSSQL_PORT = 1433;

    static GenericContainer container;
    final static String password = "yourStrong(!)Password";

    private static final String MSSQL_QUERY_LOGGER = "io.r2dbc.mssql.QUERY";

    @BeforeEach
    public void beforeEach() {
        container = new GenericContainer("mcr.microsoft.com/mssql/server:2022-CU16-ubuntu-22.04")
            .withExposedPorts(MSSQL_PORT)
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("SA_PASSWORD", password)
            .withEnv("MSSQL_COLLATION", "cyrillic_general_ci_as")
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*The default collation was successfully changed.*\\s")
                .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));

        container.start();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
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
        properties.setResourcesPath("classpath:/migrations/mssql/*.sql");

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null, null).block();

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
        properties.setResourcesPath("classpath:/migrations/mssql/*.sql");
        properties.setValidationQuery("SELECT collation_name as validation_result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null, null).block();

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
        properties.setResourcesPath("classpath:/migrations/mssql/*.sql");
        properties.setValidationQuery("SELECT collation_name as validation_result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");
        properties.setMigrationsSchema("my scheme");
        properties.setMigrationsTable("my migrations");
        properties.setMigrationsLockTable("my migrations lock");

        ConnectionFactory connectionFactory = makeConnectionMono(mappedPort);

        Mono<Long> integerMono = Mono.usingWhen(
            connectionFactory.create(),
            connection -> Mono
                .from(connection.createStatement("create schema \"my scheme\"").execute())
                .flatMap(o -> Mono.from(o.getRowsUpdated())),
            Connection::close
        );
        integerMono.block();

        R2dbcMigrate.migrate(connectionFactory, properties, springResourceReader, null, null).block();

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


        Flux<MigrationMetadata> miFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from \"my scheme\".\"my migrations\" order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new MigrationMetadata(
                        row.get("id", Integer.class),
                        row.get("description", String.class),
                        false,
                        false,
                        false,
                        false
                    );
                })),
            Connection::close
        );
        List<MigrationMetadata> migrationInfos = miFlux.collectList().block();
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
        properties.setResourcesPath("classpath:/migrations/mssql/*.sql");
        properties.setValidationQuery("SELECT collation_name as validation_result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");

        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null, null).block();

        // here we simulate new launch
        properties.setResourcesPath("classpath:/migrations/mssql_append/*.sql");
        // and we assert that we able to add yet another database (nontransactional should work)
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null, null).block();
    }

    @Test
    public void testThatLockIsReleasedAfterError() {
        // create and start a ListAppender
        try (LogCaptor logCaptor = LogCaptor.forName(MSSQL_QUERY_LOGGER)) {
            logCaptor.setLogLevelToDebug();

            R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
            properties.setDialect(Dialect.MSSQL);
            properties.setResourcesPath("classpath:/migrations/mssql_error/*.sql");

            Integer mappedPort = container.getMappedPort(MSSQL_PORT);

            RuntimeException thrown = Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null, null).block();
                },
                "Expected exception to throw, but it didn't"
            );
            Assertions.assertTrue(thrown.getMessage().contains("Incorrect syntax near 'schyachne'."));

            // make asserts
            assertTrue(
                hasSubList(logCaptor.getDebugLogs(), Arrays.asList(
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
            logCaptor.setLogLevelToInfo();
        }
    }

    private static boolean hasSubList(List<String> collect, List<String> sublist) {
        sublist = sublist.stream().map(s -> "Executing query: " + s).collect(Collectors.toList());
        return (Collections.indexOfSubList(collect, sublist) != -1);
    }
}
