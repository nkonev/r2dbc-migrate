package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
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
    public io.r2dbc.spi.Statement tryAcquireLock(Connection connection) {
        return connection.createStatement("select pg_try_advisory_lock($1, $2) as lock_result")
                .bind("$1", migrationsSchemaHashCode)
                .bind("$2", migrationsLockTableHashCode);
    }

    @Override
    public io.r2dbc.spi.Statement releaseLock(Connection connection) {
        return connection.createStatement("select pg_advisory_unlock($1, $2)")
                .bind("$1", migrationsSchemaHashCode)
                .bind("$2", migrationsLockTableHashCode);
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
