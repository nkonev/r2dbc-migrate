package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;

public class MysqlTestcontainersTest extends AbstractMysqlLikeTestcontainersTest {

    final static int MYSQL_PORT = 3306;

    private GenericContainer container;

    final static String user = "root";
    final static String password = "my-secret-pw";

    @BeforeEach
    public void beforeEach() {
        container = new GenericContainer("mysql:8.4.3")
            .withClasspathResourceMapping("/docker/mysql/docker-entrypoint-initdb.d", "/docker-entrypoint-initdb.d", BindMode.READ_ONLY)
            .withEnv("MYSQL_ROOT_PASSWORD", password)
            .withExposedPorts(MYSQL_PORT)
            .withCommand("--character-set-server=utf8", "--collation-server=utf8_unicode_ci", "--mysql-native-password=ON")
            .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds));

        container.start();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
    }

    protected ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(PORT, port)
            .option(USER, user)
            .option(PASSWORD, password)
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("sslMode"), "disabled")
            .build());
        return connectionFactory;
    }

    @Override
    protected int getMappedPort() {
        return container.getMappedPort(MYSQL_PORT);
    }

    @Override
    protected LogCaptor getStatementsLogger() {
        return LogCaptor.forClass(R2dbcMigrate.class);
    }

}
