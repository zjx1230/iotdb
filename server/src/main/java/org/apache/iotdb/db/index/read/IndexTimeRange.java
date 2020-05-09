package org.apache.iotdb.db.index.read;

import java.util.List;
import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeLt;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class IndexTimeRange {


  private Filter timeFilter;



  public IndexTimeRange() {
    timeFilter = TimeFilter.gt(Long.MAX_VALUE);
  }

  public IndexTimeRange(Filter timeFilter) {
    if (timeFilter == null) {
      this.timeFilter = TimeFilter.gt(Long.MAX_VALUE);
    } else {
      this.timeFilter = timeFilter;
    }
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

  public boolean intersect(long startTime, long endTime) {
    return timeFilter.satisfyStartEndTime(startTime, endTime);
  }

  public static Filter toFilter(long startTime, long endTime) {
    return FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }


  public Filter getTimeFilter() {
    return timeFilter;
  }
  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Override
  public String toString() {
    return timeFilter == null ? "null" : timeFilter.toString();
  }


}
