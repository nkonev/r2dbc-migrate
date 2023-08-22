package name.nkonev.r2dbc.migrate.reader;

import java.io.IOException;
import java.io.InputStream;

public class SpringResource implements MigrateResource {

    private final org.springframework.core.io.Resource springResource;

    public SpringResource(org.springframework.core.io.Resource springResource) {
        this.springResource = springResource;
    }

    @Override
    public boolean isReadable() {
        return springResource.isReadable();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return springResource.getInputStream();
    }

    @Override
    public String getFilename() {
        return springResource.getFilename();
    }

    @Override
    public String toString() {
        return springResource.toString();
    }
}
