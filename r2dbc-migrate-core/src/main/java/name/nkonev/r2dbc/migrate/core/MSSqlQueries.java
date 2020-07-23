package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.List;
import org.springframework.util.StringUtils;

public class MSSqlQueries implements SqlQueries {

    private final String migrationsSchema;
    private final String migrationsTable;
    private final String migrationsLockTable;

    public MSSqlQueries(String migrationsSchema, String migrationsTable, String migrationsLockTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsTable = migrationsTable;
        this.migrationsLockTable = migrationsLockTable;
    }

    private boolean schemaIsDefined() {
        return !StringUtils.isEmpty(migrationsSchema);
    }

    private String quoteAsString(String input) {
        return "'" + input + "'";
    }

    private String quoteAsObject(String input) {
        return "\"" + input + "\"";
    }

    @Override
    public List<String> createInternalTables() {
        if (schemaIsDefined()) {
            return Arrays.asList(
                    String.format("if not exists (select * from sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = %s and t.name = %s) ", quoteAsString(migrationsSchema), quoteAsString(migrationsTable))
                        + String.format("create table %s.%s (id int primary key, description text)", quoteAsObject(migrationsSchema), quoteAsObject(migrationsTable)),

                    String.format("if not exists (select * from sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = %s and t.name = %s) ", quoteAsString(migrationsSchema), quoteAsString(migrationsLockTable))
                        + String.format("create table %s.%s (id int primary key, locked bit not null)", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable)),

                    String.format("if not exists (select * from %s.%s where id = 1) ", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable)) // dbo.migrations_lock
                            + String.format("insert into %s.%s(id, locked) values (1, 'false')", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable))
            );
        } else {
            return Arrays.asList(
                String.format("if not exists (SELECT 1 FROM sys.Tables WHERE Name = N%s AND Type = N'U') ", quoteAsString(migrationsTable))
                    + String.format("create table %s (id int primary key, description text)", quoteAsObject(migrationsTable)),
                String.format("if not exists (SELECT 1 FROM sys.Tables WHERE Name = N%s AND Type = N'U') ", quoteAsString(migrationsLockTable))
                    + String.format("create table %s (id int primary key, locked bit not null)", quoteAsObject(migrationsLockTable)),
                String.format("if not exists (select * from %s where id = 1) ", quoteAsObject(migrationsLockTable))
                    + String.format("insert into %s (id, locked) values (1, 'false')", quoteAsObject(migrationsLockTable))
            );
        }

    }

    @Override
    public String getMaxMigration() {
        if (schemaIsDefined()) {
            return String.format("select max(id) as max from %s.%s", quoteAsObject(migrationsSchema), quoteAsObject(migrationsTable));
        } else {
            return String.format("select max(id) as max from %s", quoteAsObject(migrationsTable));
        }
    }

    public String insertMigration() {
        if (schemaIsDefined()) {
            return String.format("insert into %s.%s(id, description) values (@id, @descr)", quoteAsObject(migrationsSchema), quoteAsObject(migrationsTable));
        } else {
            return String.format("insert into %s(id, description) values (@id, @descr)", quoteAsObject(migrationsTable));
        }
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
        if (schemaIsDefined()) {
            return String.format("update %s.%s set locked = 'true' where id = 1 and locked = 'false'", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable));
        } else {
            return String.format("update %s set locked = 'true' where id = 1 and locked = 'false'", quoteAsObject(migrationsLockTable));
        }
    }

    @Override
    public String releaseLock() {
        if (schemaIsDefined()) {
            return String.format("update %s.%s set locked = 'false' where id = 1", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable));
        } else {
            return String.format("update %s set locked = 'false' where id = 1", quoteAsObject(migrationsLockTable));
        }
    }
}
