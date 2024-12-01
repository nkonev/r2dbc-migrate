package name.nkonev.r2dbc.migrate.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class MigrationMetadataFactoryTest {

    @Test
    void testSplit() {
        String s = "V5__create_customers__split.sql";
        MigrationMetadata migrationInfo = MigrationMetadataFactory.getMigrationMetadata(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertTrue(migrationInfo.isSplitByLine());
        Assertions.assertTrue(migrationInfo.isTransactional());
    }

    @Test
    void testSplitNontransactional() {
        String s = "V5__create_customers__split,nontransactional.sql";
        MigrationMetadata migrationInfo = MigrationMetadataFactory.getMigrationMetadata(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertTrue(migrationInfo.isSplitByLine());
        Assertions.assertFalse(migrationInfo.isTransactional());
    }

    @Test
    void testNontransactional() {
        String s = "V5__create_customers__nontransactional.sql";
        MigrationMetadata migrationInfo = MigrationMetadataFactory.getMigrationMetadata(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertFalse(migrationInfo.isSplitByLine());
        Assertions.assertFalse(migrationInfo.isTransactional());
    }

    @Test
    void testPremigrateNontransactional() {
        String s = "V-1__create_schemas__premigration,nontransactional.sql";
        MigrationMetadata migrationInfo = MigrationMetadataFactory.getMigrationMetadata(s);
        Assertions.assertEquals(-1, migrationInfo.getVersion());
        Assertions.assertEquals("create schemas", migrationInfo.getDescription());
        Assertions.assertFalse(migrationInfo.isSplitByLine());
        Assertions.assertFalse(migrationInfo.isTransactional());
        Assertions.assertTrue(migrationInfo.isPremigration());
    }

    @Test
    void test() {
        String s = "V5__create_customers.sql";
        MigrationMetadata migrationInfo = MigrationMetadataFactory.getMigrationMetadata(s);
        Assertions.assertEquals(5, migrationInfo.getVersion());
        Assertions.assertEquals("create customers", migrationInfo.getDescription());
        Assertions.assertFalse(migrationInfo.isSplitByLine());
        Assertions.assertTrue(migrationInfo.isTransactional());
    }


}