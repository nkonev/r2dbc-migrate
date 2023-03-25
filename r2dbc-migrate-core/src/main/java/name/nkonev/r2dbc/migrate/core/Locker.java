package name.nkonev.r2dbc.migrate.core;

import java.util.List;

public interface Locker {
    List<String> createInternalTables();

    String tryAcquireLock();

    String releaseLock();

}
