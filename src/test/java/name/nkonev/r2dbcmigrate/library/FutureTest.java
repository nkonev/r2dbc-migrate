package name.nkonev.r2dbcmigrate.library;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import reactor.core.publisher.Mono;

import java.io.File;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class FutureTest {

    final static int POSTGRESQL_PORT = 5432;
    static GenericContainer container;

    @BeforeAll
    public static void beforeAll() {
        container = new GenericContainer("postgres:12.2")
                .withExposedPorts(POSTGRESQL_PORT)
                .withEnv("POSTGRES_PASSWORD", "postgresqlPassword")
                .withClasspathResourceMapping("docker/postgresql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                        .withTimes(2));
        container.start();
    }

    @AfterAll
    public static void afterAll() {
        container.stop();
    }

    private Mono<Connection> makeConnectionMono(String host, int port, String database) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, host)
                .option(PORT, port)  // optional, defaults to 5432
                .option(USER, "r2dbc")
                .option(PASSWORD, "r2dbcPazZw0rd")
                .option(DATABASE, database)  // optional
                .build());
        Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
        return Mono.from(connectionPublisher);
    }

    @Test
    public void testThatTransactionsWrapsQueriesAndTransactionsAreNotNested() {
        R2dbcMigrate.MigrateProperties properties = new R2dbcMigrate.MigrateProperties();
        properties.setDialect(Dialect.POSTGRESQL);
        properties.setResourcesPath("file:./migrations/postgresql/*.sql");

        Integer mappedPort = container.getMappedPort(POSTGRESQL_PORT);
        R2dbcMigrate.migrate(() -> makeConnectionMono("127.0.0.1", mappedPort, "r2dbc"), properties).blockLast();

        // get log
        // make asserts
    }

    @Disabled
    @Test
    public void testSplittedLargeMigrationsFitsInMemory() {
        // _JAVA_OPTIONS: -Xmx128m
    }

    @Disabled
    @Test
    public void testCreateMsSqlDatabaseThenSchemaInItThenTableInIt() {

    }

}
