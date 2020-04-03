package name.nkonev.r2dbc.migrate.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class FilenameParserTest {

    @Test
    void testSplit() {
        String s = "V5__create_customers__split.sql";
        FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertTrue(migrationInfo.isSplitByLine());
        Assertions.assertTrue(migrationInfo.isTransactional());
    }

    @Test
    void testSplitNontransactional() {
        String s = "V5__create_customers__split,nontransactional.sql";
        FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertTrue(migrationInfo.isSplitByLine());
        Assertions.assertFalse(migrationInfo.isTransactional());
    }

    @Test
    void testNontransactional() {
        String s = "V5__create_customers__nontransactional.sql";
        FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertFalse(migrationInfo.isSplitByLine());
        Assertions.assertFalse(migrationInfo.isTransactional());
    }

    @Test
    void test() {
        String s = "V5__create_customers.sql";
        FilenameParser.MigrationInfo migrationInfo = FilenameParser.getMigrationInfo(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertFalse(migrationInfo.isSplitByLine());
        Assertions.assertTrue(migrationInfo.isTransactional());
    }


}