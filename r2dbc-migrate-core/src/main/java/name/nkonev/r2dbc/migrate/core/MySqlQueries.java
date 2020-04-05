package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.List;

public class MySqlQueries implements SqlQueries {

    @Override
    public List<String> createInternalTables() {
        return Arrays.asList(
                "create table if not exists migrations (id int primary key, description text)",
                "create table if not exists migrations_lock (id int primary key, locked boolean not null)",
                "insert ignore into migrations_lock(id, locked) values (1, false)"
        );
    }

    @Override
    public String getMaxMigration() {
        return "select max(id) as max from migrations";
    }

    public String insertMigration() {
        return "insert into migrations(id, description) values (?id, ?descr)";
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
                .createStatement(insertMigration())
                .bind("id", migrationInfo.getVersion())
                .bind("descr", migrationInfo.getDescription());
    }

    @Override
    public String tryAcquireLock() {
        return "update migrations_lock set locked = true where id = 1 and locked = false";
    }

    @Override
    public String releaseLock() {
        return "update migrations_lock set locked = false where id = 1";
    }
}
