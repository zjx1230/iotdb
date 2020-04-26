package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

public class NaiveIndexRangeStrategy extends IndexRangeStrategy {

  /**
   * NaiveIndexRangeStrategy will build index regardless the timestamp and value distribution.
   */
  @Override
  public boolean needBuildIndex(TVList sortedTVList, long buildStartTime) {
    return true;
  }

  @Override
  public long[] calculateIndexRange(TVList sortedTVList, long buildStartTime) {
    long minTime = sortedTVList.getMinTime();
    long maxTime = sortedTVList.getLastTime();
    if (maxTime < buildStartTime) {
      return new long[0];
    } else {
      return new long[]{Math.min(minTime, buildStartTime), maxTime};
    }
  }
}
