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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class MysqlTestcontainersTest {

    final static int MYSQL_PORT = 3306;

    static GenericContainer container;

    final static String user = "mysql-user";
    final static String password = "mysql-password";


    @BeforeEach
    public void beforeAll()  {
        container = new GenericContainer("mysql:5.7")
                .withClasspathResourceMapping("/docker/mysql/etc/mysql/conf.d", "/etc/mysql/conf.d", BindMode.READ_ONLY)
                .withClasspathResourceMapping("/docker/mysql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .withEnv("MYSQL_ALLOW_EMPTY_PASSWORD", "true")
                .withExposedPorts(MYSQL_PORT);

        container.start();
    }

    @AfterEach
    public void afterAll() {
        container.stop();
    }

    private Mono<Connection> makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, user)
                .option(PASSWORD, password)
                .option(DATABASE, "r2dbc")
                .build());
        Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
        return Mono.from(connectionPublisher);
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

        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setDialect(Dialect.MYSQL);
        properties.setResourcesPath("classpath:/migrations/mysql/*.sql");
        R2dbcMigrate.migrate(() -> makeConnectionMono(mappedPort), properties).block();

        Flux<Customer> clientFlux = makeConnectionMono(mappedPort)
                .flatMapMany(connection -> Flux.from(connection.createStatement("select * from customer order by id").execute()).doFinally(signalType -> connection.close()))
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Customer(
                            row.get("id", Long.class),
                            row.get("first_name", String.class),
                            row.get("last_name", String.class)
                    );
                }));
        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.lastName);
        Assertions.assertEquals(6, client.id);

    }

}
