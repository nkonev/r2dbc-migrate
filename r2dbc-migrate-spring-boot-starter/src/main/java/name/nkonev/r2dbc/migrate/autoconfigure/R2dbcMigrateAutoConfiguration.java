package name.nkonev.r2dbc.migrate.autoconfigure;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import name.nkonev.r2dbc.migrate.core.R2dbcMigrate;
import name.nkonev.r2dbc.migrate.core.R2dbcMigrateProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AbstractDependsOnBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryBuilder;
import org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.autoconfigure.r2dbc.EmbeddedDatabaseConnection;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties(R2dbcMigrateAutoConfiguration.SpringBootR2dbcMigrateProperties.class)
public class R2dbcMigrateAutoConfiguration {

    private static final String MIGRATE_BEAN_NAME = "r2dbcMigrate";

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateAutoConfiguration.class);

    // declares that ConnectionFactory depends on r2dbcMigrate
    @Configuration
    public static class R2dbcConnectionFactoryDependsOnBeanFactoryPostProcessor extends AbstractDependsOnBeanFactoryPostProcessor {

        protected R2dbcConnectionFactoryDependsOnBeanFactoryPostProcessor() {
            super(ConnectionFactory.class, MIGRATE_BEAN_NAME);
        }
    }

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
    protected static Mono<Connection> makeConnectionMono(R2dbcProperties properties,
                                                         ResourceLoader resourceLoader,
                                                         ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers) {
        LOGGER.debug("Supplying connection");
        return Mono.from(createConnectionFactory(properties, resourceLoader.getClassLoader(), customizers.orderedStream().collect(Collectors.toList())).create());
    }

    @ConfigurationProperties("r2dbc.migrate")
    public static class SpringBootR2dbcMigrateProperties extends R2dbcMigrateProperties {

    }

    public static class R2dbcMigrateBlockingInvoker {
        private R2dbcProperties r2dbcProperties;
        private ResourceLoader resourceLoader;
        private ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers;
        private R2dbcMigrateProperties properties;

        public R2dbcMigrateBlockingInvoker(R2dbcProperties r2dbcProperties, ResourceLoader resourceLoader,
                                           ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers,
                                           R2dbcMigrateProperties properties) {
            this.r2dbcProperties = r2dbcProperties;
            this.resourceLoader = resourceLoader;
            this.customizers = customizers;
            this.properties = properties;
        }

        public void migrate() {
            R2dbcMigrate.migrate(() -> makeConnectionMono(r2dbcProperties, resourceLoader, customizers), properties).block();
            LOGGER.info("End of migration");
        }

    }

    @Bean(name = MIGRATE_BEAN_NAME, initMethod = "migrate")
    public R2dbcMigrateBlockingInvoker r2dbcMigrate(R2dbcProperties r2dbcProperties, ResourceLoader resourceLoader,
                                                    ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers,
                                                    SpringBootR2dbcMigrateProperties properties) {
        return new R2dbcMigrateBlockingInvoker(r2dbcProperties, resourceLoader, customizers, properties);
    }
}
