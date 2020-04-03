package name.nkonev.r2dbc.migrate.core;

import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.stream.BaseStream;

public abstract class FileReader {

    public static Flux<String> readChunked(Resource resource, Charset fileCharset) {
        try {
            return Flux.using(() -> new BufferedReader(
                            new InputStreamReader(resource.getInputStream(), fileCharset)
                    ).lines(),
                    Flux::fromStream,
                    BaseStream::close
            );
        } catch (Exception e) {
            return Flux.error(new RuntimeException("Error during get resources from '" + resource + "'", e));
        }
    }

    public static String read(Resource resource, Charset fileCharset) {
        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, fileCharset);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
    }

}
