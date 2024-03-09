package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.List;

public class MySqlQueries implements SqlQueries {

    private final String migrationsSchema;
    private final String migrationsTable;

    public MySqlQueries(String migrationsSchema, String migrationsTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsTable = migrationsTable;
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

    @Override
    public List<String> createInternalTables() {
        return List.of(
            withMigrationsTable("create table if not exists %s(id bigint primary key, description text)")
        );
    }

    @Override
    public String getMaxMigration() {
        return withMigrationsTable("select max(id) as max from %s");
    }

    public String insertMigration() {
        return withMigrationsTable("insert into %s(id, description) values (?id, ?descr)");
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
            .createStatement(insertMigration())
            .bind("id", migrationInfo.getVersion())
            .bind("descr", migrationInfo.getDescription());
    }
}
