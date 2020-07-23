package name.nkonev.r2dbc.migrate.core;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class R2dbcMigrateProperties {
    private long connectionMaxRetries = 500;
    private List<String> resourcesPaths;
    private int chunkSize = 1000;
    private Dialect dialect;
    private String validationQuery = "select '42' as result";
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

    public R2dbcMigrateProperties() {
    }

    public List<String> getResourcesPaths() {
        return resourcesPaths;
    }

    public void setResourcesPaths(List<String> resourcesPaths) {
        this.resourcesPaths = resourcesPaths;
    }

    @Deprecated
    public void setResourcesPath(String resourcesPath) {
        this.resourcesPaths = Collections.singletonList(resourcesPath);
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

    @Override
    public String toString() {
        return "R2dbcMigrateProperties{" +
            "connectionMaxRetries=" + connectionMaxRetries +
            ", resourcesPaths=" + resourcesPaths +
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
            '}';
    }
}
