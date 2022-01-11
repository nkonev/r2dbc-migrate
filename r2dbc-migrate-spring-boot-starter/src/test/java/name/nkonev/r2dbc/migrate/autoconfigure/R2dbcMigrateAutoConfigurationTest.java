package name.nkonev.r2dbc.migrate.autoconfigure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.r2dbc.core.DatabaseClient;

import java.util.List;

public class R2dbcMigrateAutoConfigurationTest {

    @SpringBootApplication
    public static class SimpleApp implements ApplicationRunner {

        @Autowired
        private DatabaseClient client;

        private List<String> result;

        @Override
        public void run(ApplicationArguments args) throws Exception {
            result = client.sql("SELECT customer_name FROM supercustomer;")
                    .map(row -> row.get("customer_name")).all().map(o -> o.toString())
                    .collectList().block();
        }

        public List<String> getResult() {
            return result;
        }
    }

    @Test
    public void testAutoconfiguringAfterInit() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SimpleApp.class);

        builder.properties("spring.r2dbc.url=r2dbc:postgresql://127.0.0.1:25433/r2dbc");
        builder.properties("spring.r2dbc.username=r2dbc");
        builder.properties("spring.r2dbc.password=r2dbcPazZw0rd");

        // see org.springframework.boot.autoconfigure.sql.init.SettingsCreator
        builder.properties("spring.sql.init.enabled=true");
        builder.properties("spring.sql.init.mode=always");
        builder.properties("spring.sql.init.schema-locations=classpath:custom/schema/postgresql/init.sql");

        builder.properties("r2dbc.migrate.resourcesPaths=classpath:custom/migrations/postgresql/*.sql");
        builder.properties("r2dbc.migrate.run-after-sql-initializer=true");

        ConfigurableApplicationContext ctx = builder.build().run();
        SimpleApp bean = ctx.getBean(SimpleApp.class);
        List<String> result = bean.getResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Bobby", result.get(0));
    }
}
