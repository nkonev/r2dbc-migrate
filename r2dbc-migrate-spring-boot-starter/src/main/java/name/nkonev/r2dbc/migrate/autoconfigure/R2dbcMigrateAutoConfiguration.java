package name.nkonev.r2dbc.migrate.autoconfigure;

import io.r2dbc.spi.ConnectionFactory;
import name.nkonev.r2dbc.migrate.autoconfigure.R2dbcMigrateAutoConfiguration.R2dbcMigrateBlockingInvoker;
import name.nkonev.r2dbc.migrate.core.R2dbcMigrate;
import name.nkonev.r2dbc.migrate.core.R2dbcMigrateProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AbstractDependsOnBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.core.DatabaseClient;

@Configuration
@EnableConfigurationProperties(R2dbcMigrateAutoConfiguration.SpringBootR2dbcMigrateProperties.class)
@ConditionalOnClass(DatabaseClient.class)
@ConditionalOnSingleCandidate(ConnectionFactory.class)
@ConditionalOnMissingBean(R2dbcMigrateBlockingInvoker.class)
@AutoConfigureAfter(R2dbcAutoConfiguration.class)
public class R2dbcMigrateAutoConfiguration {

    private static final String MIGRATE_BEAN_NAME = "r2dbcMigrate";

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateAutoConfiguration.class);

    // declares that DatabaseClient depends on r2dbcMigrate
    @Configuration
    public static class R2dbcConnectionFactoryDependsOnBeanFactoryPostProcessor extends AbstractDependsOnBeanFactoryPostProcessor {

        protected R2dbcConnectionFactoryDependsOnBeanFactoryPostProcessor() {
            super(DatabaseClient.class, MIGRATE_BEAN_NAME);
        }
    }

    @ConfigurationProperties("r2dbc.migrate")
    public static class SpringBootR2dbcMigrateProperties extends R2dbcMigrateProperties {

    }

    public static class R2dbcMigrateBlockingInvoker {
        private ConnectionFactory connectionFactory;
        private R2dbcMigrateProperties properties;

        public R2dbcMigrateBlockingInvoker(ConnectionFactory connectionFactory,
                                           R2dbcMigrateProperties properties) {
            this.connectionFactory = connectionFactory;
            this.properties = properties;
        }

        public void migrate() {
            LOGGER.info("Starting R2DBC migration");
            R2dbcMigrate.migrate(connectionFactory, properties).block();
            LOGGER.info("End of R2DBC migration");
        }

    }

    @Bean(name = MIGRATE_BEAN_NAME, initMethod = "migrate")
    public R2dbcMigrateBlockingInvoker r2dbcMigrate(ConnectionFactory connectionFactory,
                                                    SpringBootR2dbcMigrateProperties properties) {
        return new R2dbcMigrateBlockingInvoker(connectionFactory, properties);
    }
}
