package name.nkonev.r2dbc.migrate.reader;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionsClasspathResourceReader implements MigrateResourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionsClasspathResourceReader.class);

  private final String pattern;

  public ReflectionsClasspathResourceReader(String pattern) {
    this.pattern = pattern;
  }

  public ReflectionsClasspathResourceReader() {
    this(".*\\.sql");
  }

  @Override
  public List<MigrateResource> getResources(String locationPattern) {
    Reflections reflections = new Reflections(
        new ConfigurationBuilder()
            .setScanners(Scanners.Resources)
            .setUrls(ClasspathHelper.forJavaClassPath())
            .filterInputsBy(s -> s.startsWith(locationPattern))
    );
    Set<String> resources = reflections.getResources(pattern);
    resources.forEach(s -> LOGGER.debug("Got resource {}", s));
    return resources.stream().map(ReflectionsClasspathResource::new).collect(Collectors.toList());
  }

}
