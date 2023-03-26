package name.nkonev.r2dbc.migrate.core;

import java.util.List;

public class MSSqlTableLocker extends AbstractTableLocker implements Locker {

    private final String migrationsSchema;
    private final String migrationsLockTable;

    public MSSqlTableLocker(String migrationsSchema, String migrationsLockTable) {
        this.migrationsSchema = migrationsSchema;
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

    private String withMigrationsLockTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsLockTable));
        } else {
            return String.format(template, quoteAsObject(migrationsLockTable));
        }
    }

    @Override
    public List<String> createInternalTables() {
        if (schemaIsDefined()) {
            return List.of(
                    String.format("if not exists (select * from sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = %s and t.name = %s) ", quoteAsString(migrationsSchema), quoteAsString(migrationsLockTable))
                        + String.format("create table %s.%s (id int primary key, locked bit not null)", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable)),

                    String.format("if not exists (select * from %s.%s where id = 1) ", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable)) // dbo.migrations_lock
                            + String.format("insert into %s.%s(id, locked) values (1, 'false')", quoteAsObject(migrationsSchema), quoteAsObject(migrationsLockTable))
            );
        } else {
            return List.of(
                String.format("if not exists (SELECT 1 FROM sys.Tables WHERE Name = N%s AND Type = N'U') ", quoteAsString(migrationsLockTable))
                    + String.format("create table %s (id int primary key, locked bit not null)", quoteAsObject(migrationsLockTable)),
                String.format("if not exists (select * from %s where id = 1) ", quoteAsObject(migrationsLockTable))
                    + String.format("insert into %s (id, locked) values (1, 'false')", quoteAsObject(migrationsLockTable))
            );
        }

    }

    @Override
    public String tryAcquireLock() {
        return withMigrationsLockTable("update %s set locked = 'true' where id = 1 and locked = 'false'");
    }

    @Override
    public String releaseLock() {
        return withMigrationsLockTable("update %s set locked = 'false' where id = 1");
    }
}
