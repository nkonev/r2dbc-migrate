package name.nkonev.r2dbcmigrate;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
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
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Scanner;
import java.util.stream.BaseStream;

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
        private int chunkSize;

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

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
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

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

    private static Flux<String> fromResource(Resource resource) {
        try {
            return fromPath(resource.getFile().toPath());
        } catch (IOException e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resource + "'", e));
        }
    }

    private static String getString(Resource resource) {
        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
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
                        Flux<Tuple2<Resource, FilenameParser.MigrationInfo>> resources = getResources(properties.getResourcesPath()).map(resource -> {
                            LOGGER.debug("Processing {}", resource);
                            FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(resource.getFilename());
                            LOGGER.info("Processing {}", migrationInfo);
                            return Tuples.of(resource, migrationInfo);
                        });

                        return resources.flatMap(resourceTuple -> {
                            Resource resource = resourceTuple.getT1();
                            FilenameParser.MigrationInfo migrationInfo = resourceTuple.getT2();

                            Publisher<? extends Result> migrateResultPublisher = getMigrateResultPublisher(properties, connection, resource, migrationInfo);
                            return Flux
                                    .from(migrateResultPublisher)
                                    .flatMap(r -> Flux.from(r.getRowsUpdated())
                                            .switchIfEmpty(Mono.just(0))
                                            .map(rowsUpdated -> Tuples.of(rowsUpdated, migrationInfo))
                                    );
                        });
                    })
                    .doOnEach(tuple2Signal -> {
                        if (tuple2Signal.hasValue()) {
                            Tuple2<Integer, FilenameParser.MigrationInfo> objects = tuple2Signal.get();
                            LOGGER.info("{}: {} rows affected", objects.getT2(), objects.getT1());
                        }
                    })
                    .blockLast();

            LOGGER.info("End of migration");
        };
    }

    private Publisher<? extends Result> getMigrateResultPublisher(R2DBCConfigurationProperties properties,
                                                                  Connection connection, Resource resource,
                                                                  FilenameParser.MigrationInfo migrationInfo) {
        if (migrationInfo.isSplitByLine()) {
            return fromResource(resource).buffer(properties.getChunkSize()).flatMap(strings -> {
                Batch batch = connection.createBatch();
                strings.forEach(batch::add);
                return batch.execute();
            });
        } else {
            return connection.createStatement(getString(resource)).execute();
        }
    }
}
