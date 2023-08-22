package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.List;

public class MSSqlQueries implements SqlQueries {

    private final String migrationsSchema;
    private final String migrationsTable;

    public MSSqlQueries(String migrationsSchema, String migrationsTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsTable = migrationsTable;
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

    private String withMigrationsTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsTable));
        } else {
            return String.format(template, quoteAsObject(migrationsTable));
        }
    }

    @Override
    public List<String> createInternalTables() {
        if (schemaIsDefined()) {
            return List.of(
                String.format("if not exists (select * from sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = %s and t.name = %s) ", quoteAsString(migrationsSchema), quoteAsString(migrationsTable))
                    + String.format("create table %s.%s (id int primary key, description text)", quoteAsObject(migrationsSchema), quoteAsObject(migrationsTable))

            );
        } else {
            return List.of(
                String.format("if not exists (SELECT 1 FROM sys.Tables WHERE Name = N%s AND Type = N'U') ", quoteAsString(migrationsTable))
                    + String.format("create table %s (id int primary key, description text)", quoteAsObject(migrationsTable))
            );
        }

    }

    @Override
    public String getMaxMigration() {
        return withMigrationsTable("select max(id) from %s");
    }

    public String insertMigration() {
        return withMigrationsTable("insert into %s(id, description) values (@id, @descr)");
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
            .createStatement(insertMigration())
            .bind("@id", migrationInfo.getVersion())
            .bind("@descr", migrationInfo.getDescription());
    }
}
