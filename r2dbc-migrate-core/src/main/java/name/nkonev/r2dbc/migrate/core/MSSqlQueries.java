package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.List;

public class MSSqlQueries implements SqlQueries {

    @Override
    public List<String> createInternalTables() {
        return Arrays.asList(
                "if not exists (select * from sysobjects where name='migrations' and xtype='U') create table migrations (id int primary key, description text)",
                "if not exists (select * from sysobjects where name='migrations_lock' and xtype='U') create table migrations_lock (id int primary key, locked bit not null)",
                "if not exists (select * from migrations_lock where id = 1) insert into migrations_lock(id, locked) values (1, 'false')"
        );
    }

    @Override
    public String getMaxMigration() {
        return "select max(id) as max from migrations";
    }

    public String insertMigration() {
        return "insert into migrations(id, description) values (@id, @descr)";
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
                .createStatement(insertMigration())
                .bind("@id", migrationInfo.getVersion())
                .bind("@descr", migrationInfo.getDescription());
    }

    @Override
    public String tryAcquireLock() {
        return "update migrations_lock set locked = 'true' where id = 1 and locked = 'false'";
    }

    @Override
    public String releaseLock() {
        return "update migrations_lock set locked = 'false' where id = 1";
    }
}
