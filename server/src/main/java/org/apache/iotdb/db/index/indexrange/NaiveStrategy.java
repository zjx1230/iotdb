package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * As long as {@code configStartTime} overlaps over the {@code ratio} (default 0.8) with the time
 * range of {@code sortedTVList}, NaiveIndexRangeStrategy will build the index.
 */
public class NaiveStrategy extends DefaultStrategy {

  @Override
  public boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime) {
    long currentStartTime = sortedTVList.getTime(offset);
    long lastTime = sortedTVList.getLastTime();
    return (float) (lastTime - currentStartTime) / (lastTime - configStartTime) > 0.8;
  }

}
