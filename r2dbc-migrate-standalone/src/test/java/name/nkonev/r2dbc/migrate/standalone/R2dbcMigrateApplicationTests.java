package name.nkonev.r2dbc.migrate.standalone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.test.context.SpringBootTest;

@EnabledIfSystemProperty(named = "enableStandaloneTests", matches = "true")
@SpringBootTest(classes=R2dbcMigrateApplication.class)
class R2dbcMigrateApplicationTests {

	@Test
	void contextLoads() {
	}

}
