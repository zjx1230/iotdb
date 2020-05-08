package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeLt;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class IndexTimeRange {

  private Filter timeFilter;

  public IndexTimeRange() {
    timeFilter = TimeFilter.lt(Long.MAX_VALUE);
  }


  public void addRange(long startTime, long endTime) {
    timeFilter = FilterFactory.or(timeFilter, toFilter(startTime, endTime));
  }

  private Filter toFilter(long startTime, long endTime) {
    return FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }

  public void pruneRange(long startTime, long endTime) {
    timeFilter = FilterFactory.and(timeFilter, TimeFilter.not(toFilter(startTime, endTime)));
  }


  public void updateUsableRange(IndexTimeRange usableRangeInCurrentChunk) {
    throw new UnsupportedOperationException();
  }
}
