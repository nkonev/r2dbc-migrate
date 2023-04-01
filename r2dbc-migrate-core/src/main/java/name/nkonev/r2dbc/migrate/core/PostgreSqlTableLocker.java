package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;

import java.util.List;

public class PostgreSqlTableLocker extends AbstractTableLocker implements Locker {

    private final String migrationsSchema;
    private final String migrationsLockTable;

    public PostgreSqlTableLocker(String migrationsSchema, String migrationsLockTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsLockTable = migrationsLockTable;
    }

    private boolean schemaIsDefined() {
        return !StringUtils.isEmpty(migrationsSchema);
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
        return List.of(
            withMigrationsLockTable("create table if not exists %s(id int primary key, locked boolean not null)"),
            withMigrationsLockTable("insert into %s(id, locked) values (1, false) on conflict (id) do nothing")
        );
    }

    @Override
    public io.r2dbc.spi.Statement tryAcquireLock(Connection connection) {
        return connection.createStatement(withMigrationsLockTable("update %s set locked = true where id = 1 and locked = false"));
    }

    @Override
    public io.r2dbc.spi.Statement releaseLock(Connection connection) {
        return connection.createStatement(withMigrationsLockTable("update %s set locked = false where id = 1"));
    }
}
