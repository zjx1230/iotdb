package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * The default choice. Preprocessor can custom other strategy.
 * Build index for all range, even if there is only one point.
 */
public class DefaultStrategy extends IndexRangeStrategy {


  @Override
  public boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime) {
    return sortedTVList.getTime(offset) >= configStartTime;
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
