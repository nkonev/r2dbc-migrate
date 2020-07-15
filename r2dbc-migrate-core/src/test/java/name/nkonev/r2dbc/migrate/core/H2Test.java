package name.nkonev.r2dbc.migrate.core;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static name.nkonev.r2dbc.migrate.core.ListUtils.hasSubList;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class H2Test extends LogCaptureableTests {

    static Logger statementsLogger;
    static Level statementsPreviousLevel;

    @BeforeEach
    public void beforeEach()  {
        statementsLogger = (Logger) LoggerFactory.getLogger("name.nkonev.r2dbc.migrate.core.R2dbcMigrate");
        statementsPreviousLevel = statementsLogger.getEffectiveLevel();
    }

    @AfterEach
    public void afterEach() {
        statementsLogger.setLevel(statementsPreviousLevel);
    }

    private ConnectionFactory makeConnectionMono() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "h2")
                .option(PROTOCOL, "mem")
                .option(DATABASE, "r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1")
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

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setDialect(Dialect.H2);
        properties.setResourcesPath("classpath:/migrations/h2/*.sql");
        R2dbcMigrate.migrate(makeConnectionMono(), properties).block();

        Flux<Customer> clientFlux = Mono.from(makeConnectionMono().create())
                .flatMapMany(connection -> Flux.from(connection.createStatement("select * from customer order by id").execute()).doFinally(signalType -> connection.close()))
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Customer(
                            Long.valueOf(row.get("id", Integer.class)),
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
