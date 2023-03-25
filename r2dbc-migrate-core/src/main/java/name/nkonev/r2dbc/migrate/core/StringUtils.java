package name.nkonev.r2dbc.migrate.core;

abstract class StringUtils {
  public static boolean isEmpty(Object str) {
    return (str == null || "".equals(str));
  }
}
