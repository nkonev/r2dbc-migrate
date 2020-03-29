package name.nkonev.r2dbcmigrate.runner;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import name.nkonev.r2dbcmigrate.library.R2dbcMigrate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryBuilder;
import org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.autoconfigure.r2dbc.EmbeddedDatabaseConnection;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

// https://github.com/spring-projects/spring-data-r2dbc
// https://spring.io/blog/2019/12/06/spring-data-r2dbc-goes-ga
// https://medium.com/w-logs/jdbc-for-spring-webflux-spring-data-r2dbc-99690208cfeb
// https://www.infoq.com/news/2018/10/springone-r2dbc/
// https://spring.io/blog/2018/12/07/reactive-programming-and-relational-databases
// https://www.baeldung.com/r2dbc
// https://simonbasle.github.io/2017/10/file-reading-in-reactor/
// https://r2dbc.io/spec/0.8.1.RELEASE/spec/html/
// https://www.infoq.com/news/2018/10/springone-r2dbc/
@EnableConfigurationProperties(R2dbcMigrateApplication.SpringBootMigrateProperties.class)
@SpringBootApplication
public class R2dbcMigrateApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateApplication.class);

    // copy-paste from org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryConfigurations
    protected static ConnectionFactory createConnectionFactory(R2dbcProperties properties, ClassLoader classLoader,
                                                               List<ConnectionFactoryOptionsBuilderCustomizer> optionsCustomizers) {
        return ConnectionFactoryBuilder.of(properties, () -> EmbeddedDatabaseConnection.get(classLoader))
                .configure((options) -> {
                    for (ConnectionFactoryOptionsBuilderCustomizer optionsCustomizer : optionsCustomizers) {
                        optionsCustomizer.customize(options);
                    }
                }).build();
    }

    // Connection supplier creating new Connection from new ConnectionFactory.
    // It's intentionally behaviour, see explanation in R2dbcMigrate#migrate
    protected static Mono<Connection> connectionSupplier(R2dbcProperties properties, ResourceLoader resourceLoader, ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers) {
        LOGGER.debug("Supplying connection");
        return Mono.from(createConnectionFactory(properties, resourceLoader.getClassLoader(), customizers.orderedStream().collect(Collectors.toList())).create());
    }

    @ConfigurationProperties("r2dbc.migrate")
    public static class SpringBootMigrateProperties extends R2dbcMigrate.MigrateProperties {

    }

    public static void main(String[] args) {
        SpringApplication.run(R2dbcMigrateApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandlineEntryPoint(R2dbcProperties r2dbcProperties, ResourceLoader resourceLoader,
                                                   ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers,
                                                   R2dbcMigrate.MigrateProperties properties) {
        return args -> {
            new R2dbcMigrate().migrate(()->connectionSupplier(r2dbcProperties, resourceLoader, customizers), properties).blockLast();
            LOGGER.info("End of migration");
        };
    }

}
