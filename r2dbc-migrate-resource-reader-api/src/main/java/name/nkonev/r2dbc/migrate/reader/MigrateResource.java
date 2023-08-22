package name.nkonev.r2dbc.migrate.reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Something from where we can get InputStream
 */
public interface MigrateResource {

    boolean isReadable();

    InputStream getInputStream() throws IOException;

    String getFilename();
}
