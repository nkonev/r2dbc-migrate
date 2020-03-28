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
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Backoff;
import reactor.retry.Retry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

// https://github.com/spring-projects/spring-data-r2dbc
// https://spring.io/blog/2019/12/06/spring-data-r2dbc-goes-ga
// https://medium.com/w-logs/jdbc-for-spring-webflux-spring-data-r2dbc-99690208cfeb
// https://www.infoq.com/news/2018/10/springone-r2dbc/
// https://spring.io/blog/2018/12/07/reactive-programming-and-relational-databases
// https://www.baeldung.com/r2dbc
// https://simonbasle.github.io/2017/10/file-reading-in-reactor/
// https://r2dbc.io/spec/0.8.0.M8/spec/html/
@EnableConfigurationProperties(R2dbcMigrateApplication.R2DBCConfigurationProperties.class)
@SpringBootApplication
public class R2dbcMigrateApplication {

    @ConfigurationProperties("r2dbc")
    static class R2DBCConfigurationProperties {
        private String url;
        private String user;
        private String password;
        private long connectionMaxRetries;
        private String resourcesPath;

        public String getResourcesPath() {
            return resourcesPath;
        }

        public void setResourcesPath(String resourcesPath) {
            this.resourcesPath = resourcesPath;
        }

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

        public long getConnectionMaxRetries() {
            return connectionMaxRetries;
        }

        public void setConnectionMaxRetries(long connectionMaxRetries) {
            this.connectionMaxRetries = connectionMaxRetries;
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

    private static Flux<Resource> getResources(String resourcesPath) {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources;
        try {
            resources = resolver.getResources(resourcesPath);
        } catch (IOException e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resourcesPath + "'", e));
        }
        return Flux.just(resources);
    }

    @Bean
    public CommandLineRunner demo(ConnectionFactory connectionFactory, R2DBCConfigurationProperties properties) {
        LOGGER.info("Got connectionFactory");
        return (args) -> {
            Mono.from(connectionFactory.create())
                    .retryWhen(Retry.anyOf(Exception.class).backoff(Backoff.fixed(Duration.ofSeconds(1))).retryMax(properties.getConnectionMaxRetries()).doOnRetry(objectRetryContext -> {
                        LOGGER.warn("Retrying to get database connection to url {}", properties.getUrl());
                    }))
                    .flatMapMany(connection -> {
                        Flux<Resource> resourceFlux = getResources(properties.getResourcesPath()).doOnNext(resource -> {
                            LOGGER.debug("Processing {}", resource);
                            FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(resource.getFilename());
                            LOGGER.info("Got {}", migrationInfo);
                        });
                        return resourceFlux.flatMap(resource -> {
                            try(InputStream inputStream = resource.getInputStream()) {
                                String sql = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
                                return connection.createStatement(sql).execute();
                            } catch (IOException e) {
                                return Flux.error(new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e));
                            }
                        });
                    })
                    .blockLast();

            LOGGER.info("End of migration");
        };
    }
}
