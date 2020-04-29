package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * The star discrepancy is the most well-known measure for the uniformity of point distributions. If
 * the sample points are reasonably equi-distributed, it is expected that the proportion of sample
 * points that fall within that smaller square is proportionately smaller.
 *
 * For efficiency, we calculate the discrepancy only on one-resolution grids. The discrepancy is
 * normalized. A large discrepancy indicates poorly distributed sets.
 */
public class UniformStrategy extends IndexRangeStrategy {

  @Override
  public boolean needBuildIndex(TVList sortedTVList, long configStartTime) {
    long st = sortedTVList.getMinTime();
    long end = sortedTVList.getLastTime();
    long interval = (end - st) / sortedTVList.size();
    int interIdx = 1;
    int target = 1;
    float discrepancy = 0;
    int currentNum = 1;
    // the first point must be inside the first interval.
    for (int i = 1; i < sortedTVList.size(); i++) {
      long time = sortedTVList.getTime(i);
      if (time > interIdx * interval) {
        interIdx++;
        discrepancy += Math.abs(target - currentNum);
        currentNum = 0;
      } else {
        currentNum++;
      }
    }
    discrepancy /= 2 * (sortedTVList.size() - 1);
    // the default threshold is 0.3, indicating that more than 70% of the points are not within
    // the corresponding interval.
    return discrepancy < 0.3;
  }

  @Override
  public long[] calculateIndexRange(TVList sortedTVList, long configStartTime) {
    long minTime = sortedTVList.getMinTime();
    long maxTime = sortedTVList.getLastTime();
    if (maxTime < configStartTime) {
      return new long[0];
    } else {
      return new long[]{Math.min(minTime, configStartTime), maxTime};
    }
  }
}
