package name.nkonev.r2dbcmigrate.library;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.BaseStream;

public abstract class FileReader {

    public static Flux<String> readChunked(Resource resource) {
        return fromResource(resource);
    }

    public static String read(Resource resource) {
        return getString(resource);
    }

    private static Flux<String> fromResource(Resource resource) {
        try {
            return Flux.using(() -> new BufferedReader(
                    new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)
                    ).lines(),
                    Flux::fromStream,
                    BaseStream::close
            );
        } catch (Exception e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resource + "'", e));
        }
    }

    private static String getString(Resource resource) {
        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
    }

   /* public static void main(String[] args) {
        FileSystemResource fileSystemResource = new FileSystemResource("/home/nkonev/migrator/postgresql_bcs/V6__vTransactions__split.sql");
        Integer count = readChunked(fileSystemResource)
                .filter(s -> "INSERT INTO dwh_persbroker.perseus.vTransactions (TradeNum, Account, Quantity, TradeDate, Exchange, InstrumentCurrency, ISIN, PaymentCurrency, Price, SourceCode, TradeDirection, ID_DEAL, TradeType, PaymentVolume, PaidCoupon, ReceivedCoupon) VALUES ('305869982', '10131', 894468.00, '2014-11-21 17:44:19.0000000', 'ММВБ', 'RUB', 'RU0009024277', 'RUB', 2276.00, 'БО РР', 'Продажа', '2425320149', 'Акция', -393.00, null, null);".equals(s))
                .
                .collectList().map(strings -> strings.size()).block();
        System.out.println(count);
    }*/

}
