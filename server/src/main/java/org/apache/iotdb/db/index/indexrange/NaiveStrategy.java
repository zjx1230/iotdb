package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * As long as {@code configStartTime} overlaps over the {@code ratio} (default 0.8) with the time
 * range of {@code sortedTVList}, NaiveIndexRangeStrategy will build the index.
 */
public class NaiveStrategy extends IndexRangeStrategy {

  @Override
  public boolean needBuildIndex(TVList sortedTVList, long configStartTime) {
    return (float) (sortedTVList.getLastTime() - configStartTime) / (sortedTVList.getLastTime()
        - sortedTVList.getMinTime()) > 0.8;
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
