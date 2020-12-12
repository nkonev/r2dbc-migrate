package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import java.util.Arrays;
import java.util.List;

public class MariadbQueries implements SqlQueries {

    private final String migrationsSchema;
    private final String migrationsTable;
    private final String migrationsLockTable;

    public MariadbQueries(String migrationsSchema, String migrationsTable, String migrationsLockTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsTable = migrationsTable;
        this.migrationsLockTable = migrationsLockTable;
    }

    private boolean schemaIsDefined() {
        return !StringUtils.isEmpty(migrationsSchema);
    }

    private String quoteAsObject(String input) {
        return "`" + input + "`";
    }

    private String withMigrationsTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsTable));
        } else {
            return String.format(template, quoteAsObject(migrationsTable));
        }
    }

    private String withMigrationsLockTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsLockTable));
        } else {
            return String.format(template, quoteAsObject(migrationsLockTable));
        }
    }

    @Override
    public List<String> createInternalTables() {
        return Arrays.asList(
            withMigrationsTable("create table if not exists %s(id int primary key, description text)"),
            withMigrationsLockTable("create table if not exists %s(id int primary key, locked boolean not null)"),
            withMigrationsLockTable("insert ignore into %s(id, locked) values (1, false)")
        );
    }

    @Override
    public String getMaxMigration() {
        return withMigrationsTable("select max(id) as max from %s");
    }

    public String insertMigration() {
        return withMigrationsTable("insert into %s(id, description) values (?, ?)");
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
                .createStatement(insertMigration())
                .bind(0, migrationInfo.getVersion())
                .bind(1, migrationInfo.getDescription());
    }

    @Override
    public String tryAcquireLock() {
        return withMigrationsLockTable("update %s set locked = true where id = 1 and locked = false");
    }

    @Override
    public String releaseLock() {
        return withMigrationsLockTable("update %s set locked = false where id = 1");
    }
}
