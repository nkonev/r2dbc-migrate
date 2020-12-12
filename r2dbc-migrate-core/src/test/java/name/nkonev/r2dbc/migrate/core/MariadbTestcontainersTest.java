package name.nkonev.r2dbc.migrate.core;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

public class MariadbTestcontainersTest extends AbstractMysqlLikeTestcontainersTest {

    final static int MYSQL_PORT = 3306;

    private GenericContainer container;

    final static String user = "mysql-user";
    final static String password = "mysql-password";

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @Override
    protected GenericContainer getContainer() {
        return container;
    }

    @BeforeEach
    public void beforeEach()  {
        container = new GenericContainer("mariadb:10.5.8-focal")
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

    protected ConnectionFactory makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mariadb")
            .option(HOST, "127.0.0.1")
            .option(PORT, port)
            .option(USER, user)
            .option(PASSWORD, password)
            .option(DATABASE, "r2dbc")
            .build());
        return connectionFactory;
    }

    @Override
    protected int getMappedPort() {
        return container.getMappedPort(MYSQL_PORT);
    }

    @Override
    protected Level getStatementsPreviousLevel() {
        return statementsPreviousLevel;
    }

    @Override
    protected Logger getStatementsLogger() {
        return statementsLogger;
    }


}
