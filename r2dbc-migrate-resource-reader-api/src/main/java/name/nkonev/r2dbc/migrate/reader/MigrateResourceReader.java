package name.nkonev.r2dbc.migrate.reader;

import java.util.List;

public interface MigrateResourceReader {
    List<MigrateResource> getResources(String resourcesPath);
}
