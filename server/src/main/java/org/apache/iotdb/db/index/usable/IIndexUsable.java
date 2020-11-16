package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;

public interface IIndexUsable {

  void addUsableRange(PartialPath fullPath,  TVList tvList);

  void addUnusableRange(PartialPath fullPath, TVList tvList);

  /**
   * 获取下面的所有不可用；
   *
   * @param indexSeries contains wildcard characters.
   * @return a list of full paths
   */
  PartialPath[] getAllUnusableSeriesForWholeMatching(PartialPath indexSeries);

  /**
   * 获取一段序列的可用区间
   * @param indexSeries a full path
   * @return a time range
   */
  long[] getUnusableRangeForSeriesMatching(PartialPath indexSeries);

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexUsable getIndexUsability() {
      return new BasicIndexUsability(null);
    }
  }
}
