package name.nkonev.r2dbc.migrate.reader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

public class SpringResourceReader implements MigrateResourceReader {

    private final ResourcePatternResolver resolver;

    public SpringResourceReader() {
        resolver = new PathMatchingResourcePatternResolver();
    }

    public SpringResourceReader(ResourcePatternResolver resourcePatternResolver) {
        resolver = resourcePatternResolver;
    }

    @Override
    public List<MigrateResource> getResources(String resourcesPath) {
        try {
            org.springframework.core.io.Resource[] resources = resolver.getResources(resourcesPath);
            return Arrays.stream(resources).map(SpringResource::new).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
