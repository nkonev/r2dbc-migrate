package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.List;

import static name.nkonev.r2dbc.migrate.core.R2dbcMigrate.getResultSafely;

public class MySqlSessionLocker implements Locker {

    private static final Logger LOGGER = Loggers.getLogger(MySqlSessionLocker.class);

    private final String lockname;

    private final int timeout; // seconds

    public MySqlSessionLocker(String migrationsSchema, String migrationsLockTable) {
        this.lockname = migrationsSchema + "." + migrationsLockTable;
        this.timeout = 5; // seconds
    }

    public MySqlSessionLocker(String migrationsSchema, String migrationsLockTable, int timeoutSec) {
        this.lockname = migrationsSchema + "." + migrationsLockTable;
        this.timeout = timeoutSec; // seconds
    }

    @Override
    public List<String> createInternalTables() {
        return List.of();
    }

    @Override
    public io.r2dbc.spi.Statement tryAcquireLock(Connection connection) {
        return connection.createStatement("select get_lock(?lockname, ?timeout) as lock_result")
            .bind("lockname", lockname)
            .bind("timeout", timeout);
    }

    @Override
    public io.r2dbc.spi.Statement releaseLock(Connection connection) {
        return connection.createStatement("select release_lock(?lockname)")
            .bind("lockname", lockname);
    }

    @Override
    public Mono<? extends Object> extractResultOrError(Mono<? extends Result> lockStatement) {
        return lockStatement.flatMap(o -> Mono.from(o.map(getResultSafely("lock_result", Integer.class, 0))))
            .flatMap(anInteger -> {
                if (anInteger == 0) {
                    return Mono.error(new RuntimeException("False result"));
                } else {
                    return Mono.just(anInteger);
                }
            }).doOnSuccess(aBoolean -> {
                LOGGER.info("Acquiring database-specific lock {}", aBoolean);
            });
    }
}
