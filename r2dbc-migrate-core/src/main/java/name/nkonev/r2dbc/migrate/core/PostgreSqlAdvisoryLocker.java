package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.List;

import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;

public class PostgreSqlAdvisoryLocker implements Locker {

    private static final Logger LOGGER = Loggers.getLogger(PostgreSqlAdvisoryLocker.class);

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
        return String.format("select pg_try_advisory_lock(%s, %s) as lock_result", migrationsSchemaHashCode, migrationsLockTableHashCode);
    }

    @Override
    public String releaseLock() {
        return String.format("select pg_advisory_unlock(%s, %s)", migrationsSchemaHashCode, migrationsLockTableHashCode);
    }

    @Override
    public Mono<? extends Object> extractResultOrError(Mono<? extends Result> lockStatement) {
        return lockStatement.flatMap(o -> Mono.from(o.map(getResultSafely("lock_result", Boolean.class, false))))
            .flatMap(aBoolean -> {
                if (!aBoolean) {
                    return Mono.error(new RuntimeException("False result"));
                } else {
                    return Mono.just(aBoolean);
                }
            }).doOnSuccess(aBoolean -> {
                LOGGER.info("Acquiring database-specific lock {}", aBoolean);
            });
    }

}
