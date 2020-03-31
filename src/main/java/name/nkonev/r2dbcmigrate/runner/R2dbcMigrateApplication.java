package name.nkonev.r2dbcmigrate.runner;

import io.r2dbc.spi.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
//import reactor.tools.agent.ReactorDebugAgent;

// https://github.com/spring-projects/spring-data-r2dbc
// https://spring.io/blog/2019/12/06/spring-data-r2dbc-goes-ga
// https://medium.com/w-logs/jdbc-for-spring-webflux-spring-data-r2dbc-99690208cfeb
// https://www.infoq.com/news/2018/10/springone-r2dbc/
// https://spring.io/blog/2018/12/07/reactive-programming-and-relational-databases
// https://www.baeldung.com/r2dbc
// https://simonbasle.github.io/2017/10/file-reading-in-reactor/
// https://r2dbc.io/spec/0.8.1.RELEASE/spec/html/
// https://www.infoq.com/news/2018/10/springone-r2dbc/
@SpringBootApplication
public class R2dbcMigrateApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(R2dbcMigrateApplication.class);

    public static void main(String[] args) {
        // https://projectreactor.io/docs/core/release/reference/#reactor-tools-debug
//        ReactorDebugAgent.init();
        SpringApplication.run(R2dbcMigrateApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandlineEntryPoint(ConnectionFactory connectionFactory) {
        LOGGER.info("Here we got connectionFactory after migrations");

        return args -> { };
    }

}
