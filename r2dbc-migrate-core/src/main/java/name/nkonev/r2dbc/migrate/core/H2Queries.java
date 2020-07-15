package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import java.util.Arrays;
import java.util.List;

public class H2Queries implements SqlQueries {

  @Override
  public List<String> createInternalTables() {
    return Arrays.asList(
        "create table if not exists migrations (id int primary key, description text)",
        "create table if not exists migrations_lock (id int primary key, locked boolean not null)",
        "insert into migrations_lock(id, locked) select * from (select 1, false) x where not exists(select * from migrations_lock)"
    );
  }

  @Override
  public String getMaxMigration() {
    return "select max(id) as max from migrations";
  }

  public String insertMigration() {
    return "insert into migrations(id, description) values ($1, $2)";
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
    return "update migrations_lock set locked = true where id = 1 and locked = false";
  }

  @Override
  public String releaseLock() {
    return "update migrations_lock set locked = false where id = 1";
  }
}
