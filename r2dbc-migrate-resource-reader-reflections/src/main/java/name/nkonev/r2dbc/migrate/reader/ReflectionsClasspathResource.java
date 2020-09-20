package name.nkonev.r2dbc.migrate.reader;

import java.io.IOException;
import java.io.InputStream;

public class ReflectionsClasspathResource implements MigrateResource {

  // migrations/postgresql/V1__create_customers.sql
  private final String classpathPath;

  public ReflectionsClasspathResource(String classpathPath) {
    this.classpathPath = classpathPath;
  }

  @Override
  public boolean isReadable() {
    return true;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return getResourceAsStream(classpathPath);
  }

  @Override
  public String getFilename() {
    String[] split = classpathPath.split("/");
    return split[split.length-1];
  }

  private ClassLoader getContextClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  private InputStream getResourceAsStream(String resource) {
    final InputStream in = getContextClassLoader().getResourceAsStream(resource);
    return in == null ? getClass().getResourceAsStream(resource) : in;
  }
}
