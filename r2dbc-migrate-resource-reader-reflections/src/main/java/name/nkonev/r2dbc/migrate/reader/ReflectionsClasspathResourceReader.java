package name.nkonev.r2dbc.migrate.reader;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionsClasspathResourceReader implements MigrateResourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionsClasspathResourceReader.class);

  private final java.util.function.Predicate<String> namePredicate;

  public ReflectionsClasspathResourceReader(
      Predicate<String> namePredicate) {
    this.namePredicate = namePredicate;
  }

  public ReflectionsClasspathResourceReader() {
    this.namePredicate = s -> s.endsWith(".sql");
  }

  @Override
  public List<MigrateResource> getResources(String locationPattern) {
    Reflections reflections = new Reflections(locationPattern, new ResourcesScanner());
    Set<String> resources = reflections.getResources(namePredicate);
    resources.forEach(s -> LOGGER.debug("Got resource {}", s));
    return resources.stream().map(ReflectionsClasspathResource::new).collect(Collectors.toList());
  }

}
