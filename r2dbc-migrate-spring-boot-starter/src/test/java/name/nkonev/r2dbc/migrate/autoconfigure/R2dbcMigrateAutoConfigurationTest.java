package name.nkonev.r2dbc.migrate.autoconfigure;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static name.nkonev.r2dbc.migrate.autoconfigure.TestConstants.waitTestcontainersSeconds;

/**
 * Tests working with and without Spring Boot's init capabilities https://docs.spring.io/spring-boot/docs/2.6.0/reference/html/howto.html#howto.data-initialization.using-basic-sql-scripts.
 */
public class R2dbcMigrateAutoConfigurationTest {

    final static int POSTGRESQL_PORT = 5432;
    final static String POSTGRESQL_PASSWORD = "postgresqlPassword";

    static GenericContainer container;

    @BeforeEach
    public void beforeEach() {
        container = new GenericContainer("postgres:13.4")
            .withExposedPorts(POSTGRESQL_PORT)
            .withEnv("POSTGRES_PASSWORD", POSTGRESQL_PASSWORD)
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s")
                .withTimes(2).withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));
        container.start();
    }

    @AfterEach
    public void afterEach() {
        container.stop();
    }

    @Configuration
    @EnableAutoConfiguration
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

    @Configuration
    @EnableAutoConfiguration
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

    private static String getDbUrl() {
        return "r2dbc:postgresql://127.0.0.1:"+container.getMappedPort(POSTGRESQL_PORT);
    }
    private static String getDbFactoryUrl() {
        return "r2dbc:postgresql://" + getDbUser() + ":" + getDbPassword() + "@127.0.0.1:"+container.getMappedPort(POSTGRESQL_PORT);
    }

    private static String getDbUser() {
        return "postgres";
    }
    private static String getDbPassword() {
        return POSTGRESQL_PASSWORD;
    }


    @Test
    public void testWithInitScripts() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SimpleApp.class);

        builder.properties("spring.r2dbc.url=" + getDbUrl());
        builder.properties("spring.r2dbc.username=" + getDbUser());
        builder.properties("spring.r2dbc.password=" + getDbPassword());

        // see org.springframework.boot.autoconfigure.sql.init.SettingsCreator
        builder.properties("spring.sql.init.enabled=true");
        builder.properties("spring.sql.init.mode=always");
        builder.properties("spring.sql.init.schema-locations=classpath:custom/schema/postgresql/init.sql");

        builder.properties("r2dbc.migrate.enable=true");
        builder.properties("r2dbc.migrate.migrations-schema=gh15");
        builder.properties("r2dbc.migrate.resources-paths=classpath:custom/migrations/postgresql/*.sql");

        ConfigurableApplicationContext ctx = builder.build().run();
        SimpleApp bean = ctx.getBean(SimpleApp.class);
        List<String> result = bean.getResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Bobby", result.get(0));
    }

    @Test
    public void testWithoutInitScripts() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(getDbFactoryUrl());

        Flux.usingWhen(connectionFactory.create(),
                connection -> {
                    Batch batch = connection.createBatch();
                    batch.add("DROP SCHEMA IF EXISTS gh15noinit CASCADE;");
                    batch.add("CREATE SCHEMA gh15noinit;");
                    return Mono.from(connection.setAutoCommit(true))
                            .thenMany(
                                    Flux.from(batch.execute())
                                            .flatMap(result -> Mono.from(result.getRowsUpdated()))
                            );
                },
                Connection::close
        ).blockLast();

        SpringApplicationBuilder builder = new SpringApplicationBuilder(SimpleAppNoInit.class);

        builder.properties("spring.r2dbc.url=" + getDbUrl());
        builder.properties("spring.r2dbc.username=" + getDbUser());
        builder.properties("spring.r2dbc.password=" + getDbPassword());

        builder.properties("r2dbc.migrate.enable=true");
        builder.properties("r2dbc.migrate.migrations-schema=gh15noinit");
        builder.properties("r2dbc.migrate.resources-paths=classpath:custom/migrations/postgresql-noinit/*.sql");

        ConfigurableApplicationContext ctx = builder.build().run();
        SimpleAppNoInit bean = ctx.getBean(SimpleAppNoInit.class);
        List<String> result = bean.getResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Caddy", result.get(0));
    }
}
