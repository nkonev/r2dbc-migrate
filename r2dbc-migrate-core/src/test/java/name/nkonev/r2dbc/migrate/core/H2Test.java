package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import name.nkonev.r2dbc.migrate.reader.SpringResourceReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class H2Test {

    private static SpringResourceReader springResourceReader = new SpringResourceReader();

    private ConnectionFactory makeConnectionMono() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "h2")
                .option(PROTOCOL, "mem")
                .option(DATABASE, "r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1")
                .build());
        return connectionFactory;
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
        properties.setResourcesPaths(Collections.singletonList("classpath:/migrations/h2/*.sql"));
        R2dbcMigrate.migrate(makeConnectionMono(), properties, springResourceReader, null, null).block();

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
