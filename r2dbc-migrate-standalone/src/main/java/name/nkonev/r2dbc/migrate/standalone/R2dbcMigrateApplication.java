package name.nkonev.r2dbc.migrate.standalone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.core.DatabaseClient;
//import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
public class R2dbcMigrateApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateApplication.class);

    public static void main(String[] args) {
        // https://projectreactor.io/docs/core/release/reference/#reactor-tools-debug
//        ReactorDebugAgent.init();
        SpringApplication.run(R2dbcMigrateApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandlineEntryPoint(DatabaseClient databaseClient) {
        LOGGER.info("Here we got databaseClient after migrations");

        return args -> { };
    }

}
