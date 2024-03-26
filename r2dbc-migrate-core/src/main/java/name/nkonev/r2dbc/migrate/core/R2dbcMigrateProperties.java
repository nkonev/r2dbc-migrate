package name.nkonev.r2dbc.migrate.core;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class R2dbcMigrateProperties {
    private boolean enable = true;
    private long connectionMaxRetries = 500;
    private List<BunchOfResourcesEntry> resources = new ArrayList<>();
    private int chunkSize = 1000;
    private Dialect dialect;
    private String validationQuery = "select '42' as validation_result";
    private String validationQueryExpectedResultValue = "42";
    private Duration validationQueryTimeout = Duration.ofSeconds(5);
    private Duration validationRetryDelay = Duration.ofSeconds(1);
    private Duration acquireLockRetryDelay = Duration.ofSeconds(1);
    private long acquireLockMaxRetries = 100;
    private Charset fileCharset = StandardCharsets.UTF_8;
    private boolean waitForDatabase = true;

    private String migrationsSchema;
    private String migrationsTable = "migrations";
    private String migrationsLockTable = "migrations_lock";
    private boolean preferDbSpecificLock = true;

    public R2dbcMigrateProperties() {
    }

    public List<BunchOfResourcesEntry> getResources() {
        return resources;
    }

    public void setResources(List<BunchOfResourcesEntry> resources) {
        this.resources = resources;
    }

    public void setResourcesPath(String resourcesPath) {
        var entry = new BunchOfResourcesEntry();
        entry.setType(BunchOfResourcesType.CONVENTIONALLY_NAMED_FILES);
        entry.setResourcesPaths(Collections.singletonList(resourcesPath));
        this.resources = Collections.singletonList(entry);
    }

    public long getConnectionMaxRetries() {
        return connectionMaxRetries;
    }

    public void setConnectionMaxRetries(long connectionMaxRetries) {
        this.connectionMaxRetries = connectionMaxRetries;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    public Duration getValidationQueryTimeout() {
        return validationQueryTimeout;
    }

    public void setValidationQueryTimeout(Duration validationQueryTimeout) {
        this.validationQueryTimeout = validationQueryTimeout;
    }

    public Duration getValidationRetryDelay() {
        return validationRetryDelay;
    }

    public void setValidationRetryDelay(Duration validationRetryDelay) {
        this.validationRetryDelay = validationRetryDelay;
    }

    public Duration getAcquireLockRetryDelay() {
        return acquireLockRetryDelay;
    }

    public void setAcquireLockRetryDelay(Duration acquireLockRetryDelay) {
        this.acquireLockRetryDelay = acquireLockRetryDelay;
    }


    public long getAcquireLockMaxRetries() {
        return acquireLockMaxRetries;
    }

    public void setAcquireLockMaxRetries(long acquireLockMaxRetries) {
        this.acquireLockMaxRetries = acquireLockMaxRetries;
    }

    public Charset getFileCharset() {
        return fileCharset;
    }

    public void setFileCharset(Charset fileCharset) {
        this.fileCharset = fileCharset;
    }

    public String getValidationQueryExpectedResultValue() {
        return validationQueryExpectedResultValue;
    }

    public void setValidationQueryExpectedResultValue(String validationQueryExpectedResultValue) {
        this.validationQueryExpectedResultValue = validationQueryExpectedResultValue;
    }

    public boolean isWaitForDatabase() {
        return waitForDatabase;
    }

    public void setWaitForDatabase(boolean waitForDatabase) {
        this.waitForDatabase = waitForDatabase;
    }

    public String getMigrationsTable() {
        return migrationsTable;
    }

    public void setMigrationsTable(String migrationsTable) {
        this.migrationsTable = migrationsTable;
    }

    public String getMigrationsLockTable() {
        return migrationsLockTable;
    }

    public void setMigrationsLockTable(String migrationsLockTable) {
        this.migrationsLockTable = migrationsLockTable;
    }

    public String getMigrationsSchema() {
        return migrationsSchema;
    }

    public void setMigrationsSchema(String migrationsSchema) {
        this.migrationsSchema = migrationsSchema;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public boolean isPreferDbSpecificLock() {
        return preferDbSpecificLock;
    }

    public void setPreferDbSpecificLock(boolean preferDbSpecificLock) {
        this.preferDbSpecificLock = preferDbSpecificLock;
    }

    @Override
    public String toString() {
        return "R2dbcMigrateProperties{" +
            "enable=" + enable +
            ", connectionMaxRetries=" + connectionMaxRetries +
            ", resources.size=" + resources.size() +
            ", chunkSize=" + chunkSize +
            ", dialect=" + dialect +
            ", validationQuery='" + validationQuery + '\'' +
            ", validationQueryExpectedResultValue='" + validationQueryExpectedResultValue + '\'' +
            ", validationQueryTimeout=" + validationQueryTimeout +
            ", validationRetryDelay=" + validationRetryDelay +
            ", acquireLockRetryDelay=" + acquireLockRetryDelay +
            ", acquireLockMaxRetries=" + acquireLockMaxRetries +
            ", fileCharset=" + fileCharset +
            ", waitForDatabase=" + waitForDatabase +
            ", migrationsSchema='" + migrationsSchema + '\'' +
            ", migrationsTable='" + migrationsTable + '\'' +
            ", migrationsLockTable='" + migrationsLockTable + '\'' +
            ", preferDbSpecificLock='" + preferDbSpecificLock + '\'' +
            '}';
    }
}
