package org.apache.iotdb.db.index.performance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.junit.Test;

public class IndexTimeRangeFilterTest {

  @Test
  public void test2() {
    System.out.println(IndexUtils.removeQuotation("\'asd\'"));
    System.out.println(IndexUtils.removeQuotation("\'asd"));
    System.out.println(IndexUtils.removeQuotation("\"asd\'"));
  }

  @Test
  public void testFilter() {
    TimeFilter.TimeGt timeGt = TimeFilter.gt(500);
    TimeFilter.TimeLt timeLt = TimeFilter.lt(1000);
    AndFilter a = FilterFactory.and(timeGt, timeLt);
//    System.out.println(FilterFactory.and(timeGt, TimeFilter.lt(800)));
//    System.out.println(a.containStartEndTime(501,800));
//    System.out.println(TimeFilter.not(a));
//    System.out.println(FilterFactory.or(null, TimeFilter.lt(800)));
//    PriorityQueue<IndexChunkMeta> seqResources = new PriorityQueue<>();
//    seqResources.add(new IndexChunkMeta(1, 1, 2, 1));
//    System.out.println("asda: "+seqResources.poll());

//    List<List<Integer>> alist = new ArrayList<>();

  }

}
