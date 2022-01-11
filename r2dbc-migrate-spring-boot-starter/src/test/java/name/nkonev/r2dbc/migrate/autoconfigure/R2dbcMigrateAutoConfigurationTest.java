package name.nkonev.r2dbc.migrate.autoconfigure;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Tests working with and without Spring Boot's init capabilities https://docs.spring.io/spring-boot/docs/2.6.0/reference/html/howto.html#howto.data-initialization.using-basic-sql-scripts.
 * Those tests assume that database already instantiated, this is done by r2dbc-migration-core tests
 */
public class R2dbcMigrateAutoConfigurationTest {

    private static final String DB_URL = "r2dbc:postgresql://127.0.0.1:25433/r2dbc";
    private static final String DB_USER = "r2dbc";
    private static final String DB_PASSWORD = "r2dbcPazZw0rd";

    @SpringBootApplication
    public static class SimpleApp implements ApplicationRunner {

        @Autowired
        private DatabaseClient client;

        private List<String> result;

        @Override
        public void run(ApplicationArguments args)  {
            result = client.sql("SELECT * FROM gh15.supercustomer;")
                    .map(row -> row.get("customer_name", String.class)).all()
                    .collectList().block();
        }

        public List<String> getResult() {
            return result;
        }
    }

    //@SpringBootApplication
    public static class SimpleAppNoInit implements ApplicationRunner {

        @Autowired
        private DatabaseClient client;

        private List<String> result;

        @Override
        public void run(ApplicationArguments args)  {
            result = client.sql("SELECT * FROM gh15noinit.megacustomer;")
                    .map(row -> row.get("customer_name", String.class)).all()
                    .collectList().block();
        }

        public List<String> getResult() {
            return result;
        }
    }

    @Test
    public void testWithInitScripts() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SimpleApp.class);

        builder.properties("spring.r2dbc.url=" + DB_URL);
        builder.properties("spring.r2dbc.username=" + DB_USER);
        builder.properties("spring.r2dbc.password=" + DB_PASSWORD);

        // see org.springframework.boot.autoconfigure.sql.init.SettingsCreator
        builder.properties("spring.sql.init.enabled=true");
        builder.properties("spring.sql.init.mode=always");
        builder.properties("spring.sql.init.schema-locations=classpath:custom/schema/postgresql/init.sql");

        builder.properties("r2dbc.migrate.enable=true");
        builder.properties("r2dbc.migrate.migrations-schema=gh15");
        builder.properties("r2dbc.migrate.resourcesPaths=classpath:custom/migrations/postgresql/*.sql");

        ConfigurableApplicationContext ctx = builder.build().run();
        SimpleApp bean = ctx.getBean(SimpleApp.class);
        List<String> result = bean.getResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Bobby", result.get(0));
    }

    @Test
    public void testWithoutInitScripts() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(DB_URL + "?username="+DB_USER+"&password=" + DB_PASSWORD);
        Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
        connectionMono.map(connection -> {
            Batch batch = connection.createBatch();
            batch.add("DROP SCHEMA IF EXISTS gh15noinit CASCADE;");
            batch.add("CREATE SCHEMA gh15noinit;");
            return batch.execute();
        }).block();

        SpringApplicationBuilder builder = new SpringApplicationBuilder(SimpleAppNoInit.class);

        builder.properties("spring.r2dbc.url=" + DB_URL);
        builder.properties("spring.r2dbc.username=" + DB_USER);
        builder.properties("spring.r2dbc.password=" + DB_PASSWORD);

        builder.properties("r2dbc.migrate.enable=true");
        builder.properties("r2dbc.migrate.migrations-schema=gh15noinit");
        builder.properties("r2dbc.migrate.resourcesPaths=classpath:custom/migrations/postgresql/*.sql");

        ConfigurableApplicationContext ctx = builder.build().run();
        SimpleAppNoInit bean = ctx.getBean(SimpleAppNoInit.class);
        List<String> result = bean.getResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Caddy", result.get(0));
    }
}
