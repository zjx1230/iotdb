package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

public abstract class IndexRangeStrategy {


  /**
   * Given the TVList and the configured building start time, determine whether to build index by
   * considering the time and value distribution.
   *
   * @param sortedTVList the sorted TVList to build the index
   * @param buildStartTime the start time set by config.
   */
  public abstract boolean needBuildIndex(TVList sortedTVList, long buildStartTime);

  public abstract long[] calculateIndexRange(TVList sortedTVList, long buildStartTime);

}
