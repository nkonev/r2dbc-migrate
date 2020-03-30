package name.nkonev.r2dbcmigrate.library;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.List;

public interface SqlQueries {
    List<String> createInternalTables();

    String getMaxMigration();

    Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo);

    String tryAcquireLock();

    String releaseLock();
}
