package name.nkonev.r2dbc.migrate.core;

import java.util.Collections;
import java.util.List;

public abstract class ListUtils {
  public static boolean hasSubList(List<Object> collect, List<Object> sublist) {
    return (Collections.indexOfSubList(collect, sublist) != -1);
  }

}
