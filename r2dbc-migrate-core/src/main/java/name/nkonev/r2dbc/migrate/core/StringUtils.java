package name.nkonev.r2dbc.migrate.core;

abstract public class StringUtils {
  public static boolean isEmpty(Object str) {
    return (str == null || "".equals(str));
  }
}
