package com.duyvu.database.utils;

import com.duyvu.database.tree.SearchResult;
import java.util.Collections;
import java.util.List;

public final class SearchUtils {
  private SearchUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static <T extends Comparable<? super T>> SearchResult search(List<T> list, T key) {
    int result = Collections.binarySearch(list, key);

    if (result >= 0) {
      // Found
      return new SearchResult(true, result);
    } else {
      // Not found → recover insertion point
      int insertionPoint = -result - 1;
      return new SearchResult(false, insertionPoint);
    }
  }
}
