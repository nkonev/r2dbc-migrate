package name.nkonev.r2dbc.migrate.core;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;
import reactor.core.publisher.Flux;

@EnabledIfSystemProperty(named = "enableFuzzyStartMssqlTests", matches = "true")
public class MssqlTestcontainersConcurrentStartTest {

    final static int MSSQL_HARDCODED_PORT = 3333;

    static volatile GenericContainer container;
    final static String password = "yourStrong(!)Password";

    private static final Logger LOGGER = LoggerFactory.getLogger(MssqlTestcontainersConcurrentStartTest.class);

    static final Random random = new Random();

    private ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mssql")
            .option(HOST, "127.0.0.1")
            .option(PORT, port)
            .option(USER, "sa")
            .option(PASSWORD, password)
            .option(DATABASE, "master")
            .option(Option.valueOf("connectTimeout"), Duration.of(2, ChronoUnit.SECONDS))
            .build());
        return connectionFactory;
    }

    private ConnectionFactory makeAnotherConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mssql")
            .option(HOST, "127.0.0.1")
            .option(PORT, port)
            .option(USER, "sa")
            .option(PASSWORD, password)
            .option(DATABASE, "my_db")
            .option(Option.valueOf("connectTimeout"), Duration.of(2, ChronoUnit.SECONDS))
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

    static class Customer {
        String firstName, secondName;

        public Customer(String firstName, String secondName) {
            this.firstName = firstName;
            this.secondName = secondName;
        }
    }

    @RepeatedTest(50)
    public void testThatMigratorCanHandleSituationWhenDatabaseStillStarting() {
        int randomInteger = random.nextInt(10);
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER);
        Thread thread = new Thread(() -> {
            LOGGER.info("Sleeping random {} seconds before start the container", randomInteger);
            Uninterruptibles.sleepUninterruptibly(randomInteger, TimeUnit.SECONDS);
            container = new GenericContainer("mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-16.04")
                .withExposedPorts(MSSQL_HARDCODED_PORT)
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", password)
                .withEnv("MSSQL_TCP_PORT", ""+MSSQL_HARDCODED_PORT)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*SQL Server 2019 will run as non-root by default.*\\s") // dummy read first message
                    .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));
            container.setPortBindings(Arrays.asList(MSSQL_HARDCODED_PORT+":"+MSSQL_HARDCODED_PORT));
            container.start();
        });
        thread.setDaemon(true);
        thread.start();

        Thread thread2 = new Thread(() -> {
            for (;;) {
                try {
                    container.followOutput(logConsumer);
                    LOGGER.info("Successfully subscribed to");
                    break;
                } catch (RuntimeException i) {
                    LOGGER.warn("Unable to subscribe to");
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }
            }
        });
        thread2.setDaemon(true);
        thread2.start();


        try {
            R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
            properties.setConnectionMaxRetries(1024);
            properties.setDialect(Dialect.MSSQL);
            properties.setResourcesPath("classpath:/migrations/mssql/*.sql");
            properties.setValidationQuery("SELECT 'ololo' as result");
            properties.setValidationQueryExpectedResultValue("ololo");
            ConnectionFactory connectionFactory = makeConnectionMono(MSSQL_HARDCODED_PORT);
            R2dbcMigrate.migrate(connectionFactory, properties).block();
            ConnectionFactory connectionFactory2 = makeAnotherConnectionMono(MSSQL_HARDCODED_PORT);


            Flux<Customer> customerFlux = Flux.usingWhen(
                connectionFactory2.create(),
                connection -> Flux.from(connection.createStatement("select * from my_db.dbo.customer WHERE last_name = 'Фамилия'").execute())
                    .flatMap(o -> o.map((row, rowMetadata) -> {
                        return new Customer(
                            row.get("first_name", String.class),
                            row.get("last_name", String.class)
                        );
                    })),
                Connection::close
            );
            List<Customer> block = customerFlux.collectList().block();
            Customer customer = block.get(block.size()-1);

            Assertions.assertEquals("Покупатель", customer.firstName);
            Assertions.assertEquals("Фамилия", customer.secondName);


            Flux<Client> clientFlux = Flux.usingWhen(
                connectionFactory2.create(),
                connection -> Flux.from(connection.createStatement("select * from sales_department.rich_clients.client").execute())
                    .flatMap(o -> o.map((row, rowMetadata) -> {
                        return new Client(
                            row.get("first_name", String.class),
                            row.get("second_name", String.class),
                            row.get("account", String.class),
                            row.get("estimated_money", Integer.class)
                        );
                    })),
                Connection::close
            );
            Client client = clientFlux.blockLast();

            Assertions.assertEquals("John", client.firstName);
            Assertions.assertEquals("Smith", client.secondName);
            Assertions.assertEquals("4444", client.account);
            Assertions.assertEquals(9999999, client.estimatedMoney);
        } catch (Throwable t) {
            if (t.getMessage().contains("Move the file to a local NTFS volume")) {
                LOGGER.warn("Skipping Microsoft SQL Server Error message={}", t.getMessage());
            }
        } finally {
            if (container!=null) {
                container.stop();
            }
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}
