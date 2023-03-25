package name.nkonev.r2dbc.migrate.core;

 import java.util.List;

public class PostgreSqlAdvisoryLocker implements Locker {

    private final int migrationsSchemaHashCode;
    private final int migrationsLockTableHashCode;

    public PostgreSqlAdvisoryLocker(String migrationsSchema, String migrationsLockTable) {
        this.migrationsSchemaHashCode = (StringUtils.isEmpty(migrationsSchema) ? "42" : migrationsSchema).hashCode();
        this.migrationsLockTableHashCode = (StringUtils.isEmpty(migrationsLockTable) ? "43" : migrationsLockTable).hashCode();
    }

    @Override
    public List<String> createInternalTables() {
        return List.of();
    }

    @Override
    public String tryAcquireLock() {
        return String.format("select pg_try_advisory_lock(%s, %s)", migrationsSchemaHashCode, migrationsLockTableHashCode);
    }

    @Override
    public String releaseLock() {
        return String.format("select pg_advisory_unlock(%s, %s)", migrationsSchemaHashCode, migrationsLockTableHashCode);
    }
}
