package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

public abstract class IndexRangeStrategy {

  /**
   * Given the TVList and the configured building start time, determine whether to build index by
   * considering the time and value distribution.
   *
   * @param sortedTVList the sorted TVList to build the index
   * @param offset the offset of TVList
   * @param configStartTime user-setting start time
   */
  public abstract boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime);

  public abstract long[] calculateIndexRange(TVList sortedTVList, long buildStartTime);

}
