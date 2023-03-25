package name.nkonev.r2dbc.migrate.core;

import name.nkonev.r2dbc.migrate.reader.MigrateResource;
import reactor.core.publisher.Flux;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public abstract class FileReader {

    public static Flux<String> readChunked(MigrateResource resource, Charset fileCharset) {
        return Flux.using(
            () -> new BufferedReader(new InputStreamReader(resource.getInputStream(), fileCharset)),
            bufferedReader -> Flux.fromStream(bufferedReader.lines()),
            bufferedReader -> {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    public static String read(MigrateResource resource, Charset fileCharset) {
        try (InputStream inputStream = resource.getInputStream()) {
            return new String(inputStream.readAllBytes(), fileCharset);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
    }

}
