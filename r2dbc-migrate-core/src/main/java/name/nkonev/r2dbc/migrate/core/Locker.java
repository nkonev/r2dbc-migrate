package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;

import java.util.List;

public interface Locker {
    List<String> createInternalTables();

    io.r2dbc.spi.Statement tryAcquireLock(Connection connection);

    io.r2dbc.spi.Statement releaseLock(Connection connection);

    // sends Mono.error in case inability to acquire lock
    Mono<? extends Object> extractResultOrError(Mono<? extends Result> lockStatement);

}
