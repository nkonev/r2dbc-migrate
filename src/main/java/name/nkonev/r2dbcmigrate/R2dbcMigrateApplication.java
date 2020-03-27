package name.nkonev.r2dbcmigrate;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;

import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

// https://github.com/spring-projects/spring-data-r2dbc
// https://spring.io/blog/2019/12/06/spring-data-r2dbc-goes-ga
// https://medium.com/w-logs/jdbc-for-spring-webflux-spring-data-r2dbc-99690208cfeb
// https://www.infoq.com/news/2018/10/springone-r2dbc/
// https://spring.io/blog/2018/12/07/reactive-programming-and-relational-databases
// https://www.baeldung.com/r2dbc
@EnableConfigurationProperties(R2dbcMigrateApplication.R2DBCConfigurationProperties.class)
@SpringBootApplication
public class R2dbcMigrateApplication {

    @ConfigurationProperties("r2dbc")
    static class R2DBCConfigurationProperties {
        private String url;
        private String user;
        private String password;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateApplication.class);

    @Bean
    public ConnectionFactory connectionFactory(R2DBCConfigurationProperties properties) {
        ConnectionFactoryOptions baseOptions = ConnectionFactoryOptions.parse(properties.getUrl());
        ConnectionFactoryOptions.Builder ob = ConnectionFactoryOptions.builder().from(baseOptions);
        if (!StringUtils.isEmpty(properties.getUser())) {
            ob = ob.option(USER, properties.getUser());
        }
        if (!StringUtils.isEmpty(properties.getPassword())) {
            ob = ob.option(PASSWORD, properties.getPassword());
        }
        return ConnectionFactories.get(ob.build());
    }

    public static void main(String[] args) {
        SpringApplication.run(R2dbcMigrateApplication.class, args);
    }

    @Bean
    public CommandLineRunner demo(ConnectionFactory connectionFactory) {

        return (args) -> {
            Mono.from(connectionFactory.create())
                    .flatMapMany(it ->
                            it.createStatement("CREATE TABLE IF NOT EXISTS customer (id SERIAL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))")
                                    .execute()
                    )
                    .blockLast();

            LOGGER.info("End of migration");
        };
    }
}
