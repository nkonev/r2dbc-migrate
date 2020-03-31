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
                    new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8) // TODO make configurable
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
            return StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8); // TODO make configurable
        } catch (IOException e) {
            throw new RuntimeException("Error during reading file '" + resource.getFilename() + "'", e);
        }
    }

}
