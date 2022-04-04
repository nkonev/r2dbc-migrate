package name.nkonev.r2dbc.migrate.core;

import static name.nkonev.r2dbc.migrate.core.ListUtils.hasSubList;
import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import name.nkonev.r2dbc.migrate.core.FilenameParser.MigrationInfo;
import name.nkonev.r2dbc.migrate.reader.SpringResourceReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class AbstractMysqlLikeTestcontainersTest extends LogCaptureableTests {

    protected abstract GenericContainer getContainer();

    protected abstract ConnectionFactory makeConnectionMono(int port);

    protected abstract int getMappedPort();

    static class Customer {
        Long id;
        String firstName, lastName;

        public Customer(Long id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }

    private static SpringResourceReader springResourceReader = new SpringResourceReader();

    @Test
    public void testMigrationWorks() {
        Integer mappedPort = getMappedPort();

        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mysql/*.sql"));
        R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();

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
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mysql_error/*.sql"));

        Integer mappedPort = getMappedPort();

        RuntimeException thrown = Assertions.assertThrows(
            RuntimeException.class,
            () -> {
                R2dbcMigrate.migrate(makeConnectionMono(mappedPort), properties, springResourceReader, null).block();
            },
            "Expected exception to throw, but it didn't"
        );
        Assertions.assertTrue(thrown.getMessage().contains("You have an error in your SQL syntax; check the manual that corresponds to your"));

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


    @Test
    public void testOtherMigrationSchema() {
        R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
        properties.setMigrationsSchema("my scheme");
        properties.setMigrationsTable("my migrations");
        properties.setMigrationsLockTable("my migrations lock");
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/mysql/*.sql"));
        Integer mappedPort = getMappedPort();
        ConnectionFactory connectionFactory = makeConnectionMono(mappedPort);

        R2dbcMigrate.migrate(connectionFactory, properties, springResourceReader, null).block();

        Flux<Customer> clientFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from customer order by id").execute())
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Customer(
                        row.get("id", Long.class),
                        row.get("first_name", String.class),
                        row.get("last_name", String.class)
                    );
                })),
            Connection::close
        );
        Customer client = clientFlux.blockLast();

        Assertions.assertEquals("Customer", client.firstName);
        Assertions.assertEquals("Surname 4", client.lastName);
        Assertions.assertEquals(6, client.id);


        Flux<MigrationInfo> miFlux = Flux.usingWhen(
            connectionFactory.create(),
            connection -> Flux.from(connection.createStatement("select * from `my scheme`.`my migrations` order by id").execute())
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
        Assertions.assertEquals("create customers", migrationInfos.get(0).getDescription());

        Mono<Byte> r = Mono.usingWhen(
            makeConnectionMono(mappedPort).create(),
            connection -> Mono.from(connection.createStatement("select locked from `my scheme`.`my migrations lock` where id = 1").execute())
                .flatMap(o -> Mono.from(o.map(getResultSafely("locked", Byte.class, null)))),
            Connection::close);
        Byte block = r.block();
        Assertions.assertNotNull(block);
        Assertions.assertEquals((byte)0, block);

    }
}
