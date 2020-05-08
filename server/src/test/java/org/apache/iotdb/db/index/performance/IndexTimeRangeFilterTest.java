package org.apache.iotdb.db.index.performance;

import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.junit.Test;

public class IndexTimeRangeFilterTest {

  @Test
  public void testFilter() {
    TimeFilter.TimeGt timeGt = TimeFilter.gt(500);
    TimeFilter.TimeLt timeLt = TimeFilter.lt(1000);
    AndFilter a = FilterFactory.and(timeGt, timeLt);
    System.out.println(FilterFactory.and(timeGt, TimeFilter.lt(800)));
    System.out.println(a.containStartEndTime(501,800));
    System.out.println(TimeFilter.not(a));
  }

}
