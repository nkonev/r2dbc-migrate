package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.List;

public class PostgreSqlQueries implements SqlQueries {

    private final String migrationsTable;
    private final String migrationsLockTable;

    public PostgreSqlQueries(String migrationsTable, String migrationsLockTable) {
        this.migrationsTable = migrationsTable;
        this.migrationsLockTable = migrationsLockTable;
    }

    private String withMigrationsTable(String template) {
        return String.format(template, migrationsTable);
    }

    private String withMigrationsLockTable(String template) {
        return String.format(template, migrationsLockTable);
    }

    @Override
    public List<String> createInternalTables() {
        return Arrays.asList(
                withMigrationsTable("create table if not exists %s(id int primary key, description text)"),
                withMigrationsLockTable("create table if not exists %s(id int primary key, locked boolean not null)"),
                withMigrationsLockTable("insert into %s(id, locked) values (1, false) on conflict (id) do nothing")
        );
    }

    @Override
    public String getMaxMigration() {
        return withMigrationsTable("select max(id) from %s");
    }

    public String insertMigration() {
        return withMigrationsTable("insert into %s(id, description) values ($1, $2)");
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
                .createStatement(insertMigration())
                .bind("$1", migrationInfo.getVersion())
                .bind("$2", migrationInfo.getDescription());
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
