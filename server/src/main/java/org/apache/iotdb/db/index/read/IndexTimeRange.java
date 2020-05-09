package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeLt;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class IndexTimeRange {

  private Filter timeFilter;

  public IndexTimeRange() {
    timeFilter = TimeFilter.gt(Long.MAX_VALUE);
  }


  public void addRange(long startTime, long endTime) {
//    if (timeFilter == null) {
//      timeFilter = toFilter(startTime, endTime);
//      return;
//    }
    timeFilter = FilterFactory.or(timeFilter, toFilter(startTime, endTime));
  }

  public void pruneRange(long startTime, long endTime) {
//    if (timeFilter == null) {
//      timeFilter = TimeFilter.not(toFilter(startTime, endTime));
//    }
    timeFilter = FilterFactory.and(timeFilter, TimeFilter.not(toFilter(startTime, endTime)));
  }

  /**
   * @return True if this range fully contains [start, end]
   */
  public boolean fullyContains(long startTime, long endTime) {
    return timeFilter.containStartEndTime(startTime, endTime);
  }

  private Filter toFilter(long startTime, long endTime) {
    return FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }


  public void updateUsableRange(IndexTimeRange usableRangeInCurrentChunk) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return timeFilter == null ? "null" : timeFilter.toString();
  }
}
