package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public abstract class AbstractTableLocker implements Locker {

    private static final Logger LOGGER = Loggers.getLogger(AbstractTableLocker.class);

    @Override
    public Mono<? extends Object> extractResultOrError(Mono<? extends Result> lockStatement) {
        return lockStatement.flatMap(o -> Mono.from(o.getRowsUpdated()))
            .switchIfEmpty(Mono.just(0L))
            .flatMap(aLong -> {
                if (Integer.valueOf(0).equals(aLong)) {
                    return Mono.error(new RuntimeException("Equals zero"));
                } else {
                    return Mono.just(aLong);
                }
            }).doOnSuccess(integer -> {
                LOGGER.info("By acquiring table-based lock {} rows updated", integer);
            });
    }

}

