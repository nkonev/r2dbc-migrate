package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;

public class MssqlTestcontainersTest {

    final static int MSSQL_PORT = 1433;

    static GenericContainer container;
    final static String password = "yourStrong(!)Password";


    @BeforeEach
    public void beforeAll()  {
        container = new GenericContainer("mcr.microsoft.com/mssql/server:2017-CU19-ubuntu-16.04")
                .withExposedPorts(MSSQL_PORT)
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", password)
                .withEnv("MSSQL_COLLATION", "cyrillic_general_ci_as")
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*The default collation was successfully changed.*\\s")
                        .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));

        container.start();
    }

    @AfterEach
    public void afterAll() {
        container.stop();
    }

    private Mono<Connection> makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mssql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, "sa")
                .option(PASSWORD, password)
                .option(DATABASE, "master")
                .build());
        Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
        return Mono.from(connectionPublisher);
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

    @Test
    public void testDefaultValidationResult() {
        Integer mappedPort = container.getMappedPort(MSSQL_PORT);

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.MSSQL);
        properties.setResourcesPath("classpath:/migrations/mssql/*.sql");

        R2dbcMigrate.migrate(() -> makeConnectionMono(mappedPort), properties).block();

        Flux<Client> clientFlux = makeConnectionMono(mappedPort)
                .flatMapMany(connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute()).doFinally(signalType -> connection.close()))
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Client(
                            row.get("first_name", String.class),
                            row.get("second_name", String.class),
                            row.get("account", String.class),
                            row.get("estimated_money", Integer.class)
                    );
                }));
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
        properties.setValidationQuery("SELECT collation_name as result FROM sys.databases WHERE name = N'master'");
        properties.setValidationQueryExpectedResultValue("Cyrillic_General_CI_AS");

        R2dbcMigrate.migrate(() -> makeConnectionMono(mappedPort), properties).block();

        Flux<Client> clientFlux = makeConnectionMono(mappedPort)
            .flatMapMany(connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute()).doFinally(signalType -> connection.close()))
            .flatMap(o -> o.map((row, rowMetadata) -> {
                return new Client(
                    row.get("first_name", String.class),
                    row.get("second_name", String.class),
                    row.get("account", String.class),
                    row.get("estimated_money", Integer.class)
                );
            }));
        Client client = clientFlux.blockLast();

        Assertions.assertEquals("John", client.firstName);
        Assertions.assertEquals("Smith", client.secondName);
        Assertions.assertEquals("4444", client.account);
        Assertions.assertEquals(9999999, client.estimatedMoney);
    }

}
