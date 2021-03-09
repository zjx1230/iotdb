package org.apache.iotdb.db.index.algorithm.mmhh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.utils.Pair;


public class MMHHTest {

  private HashMap<Long, List<Long>> hashLookupTable;
  private int hashLength;

  private MMHHTest(int hashLength) {
    this.hashLookupTable = new HashMap<>();
    this.hashLength = hashLength;
  }

  private List<Pair<Integer, Long>> hammingSearch(Long queryCode, int topK) {
    List<Pair<Integer, Long>> res = new ArrayList<>();
    for (int radius = 0; radius < hashLength; radius++) {
      System.out.println("scan hamming dist = " + radius);
      boolean full = scanBucket(queryCode, 0, radius, 0, topK, res);
      if (full) {
        break;
      }
    }
    return res;
  }

  /**
   * if res has reached topK
   */
  private boolean scanBucket(long queryCode, int doneIdx, int maxIdx, int startIdx, int topK,
      List<Pair<Integer, Long>> res) {
    System.out
        .println(String.format("scan: done=%d, max=%d, start=%d, code: %d, %s",
            doneIdx, maxIdx, startIdx, queryCode, Long.toBinaryString(queryCode)));
    if (doneIdx == maxIdx) {
      if (hashLookupTable.containsKey(queryCode)) {
        List<Long> bucket = hashLookupTable.get(queryCode);
        for (Long seriesId : bucket) {
          Pair<Integer, Long> p = new Pair<>(maxIdx, seriesId);
          res.add(p);
          System.out.println(String.format("add %s, result size=%d", p, res.size()));
          if (res.size() == topK) {
            return true;
          }
        }
      }
    } else {
      for (int doIdx = startIdx; doIdx < hashLength - (maxIdx - doneIdx); doIdx++) {
        // change bit
        queryCode = reverseBit(queryCode, doIdx);
        boolean full = scanBucket(queryCode, doneIdx + 1, maxIdx, doIdx + 1, topK,
            res);
        if (full) {
          return true;
        }
        // change bit back
        queryCode = reverseBit(queryCode, doIdx);
      }
    }
    return false;
  }

  private long reverseBit(long hashCode, int idx) {
    long flag = 1L << idx;
    if ((hashCode & flag) != 0) {
      // the idx-th bit: 1 to 0
      return hashCode & ~flag;
    } else {
      // the idx-th bit: 0 to 1
      return hashCode | flag;
    }
  }

  // test hamming search
  public static void main(String[] args) {
    MMHHTest mmhh = new MMHHTest(5);
    mmhh.hashLookupTable.put(0L, Arrays.asList(0L, 1L, 2L));
    mmhh.hashLookupTable.put(1L, Arrays.asList(10L, 11L, 12L));
    mmhh.hashLookupTable.put(5L, Arrays.asList(50L, 51L, 52L));
    List<Pair<Integer, Long>> r = mmhh.hammingSearch(9L, 9);
    System.out.println(r);
  }
}
