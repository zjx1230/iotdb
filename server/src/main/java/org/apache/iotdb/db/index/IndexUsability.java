package org.apache.iotdb.db.index;

import org.apache.iotdb.db.index.read.IndexTimeRange;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class IndexUsability {

  public IndexTimeRange getIndexUsableRange() {
    return indexUsableRange;
  }

  public IndexTimeRange getAllowedRange() {
    return allowedRange;
  }

  private IndexTimeRange indexUsableRange;
  private IndexTimeRange allowedRange;

  public IndexUsability(Filter timeFilter) {
    this.indexUsableRange = new IndexTimeRange();
    // If non filter, initial allowedRange is the universal set
    this.allowedRange = new IndexTimeRange(
        timeFilter == null ? TimeFilter.lt(Long.MAX_VALUE) : timeFilter);
  }

  public void addUsableRange(long start, long end) {
    this.indexUsableRange.addRange(start, end);
  }
}
