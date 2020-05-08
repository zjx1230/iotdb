package org.apache.iotdb.db.index.read;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.common.IndexQueryException;

public class TimeRangeOld {

  private final List<Long> times = new ArrayList<>();

  public TimeRangeOld() {
  }

  public TimeRangeOld(long... times) {
    for (long time : times) {
      this.times.add(time);
    }
  }

  public void appendRange(long... usableRange) throws IndexQueryException {
    if (usableRange.length % 2 != 0) {
      throw new IndexQueryException("Length of usable range is not even: " + usableRange.length);
    }
    for (int i = 0; i < usableRange.length; i += 2) {
      long lastTime = times.get(times.size() - 1);
      if (usableRange[i] <= lastTime) {
        throw new IndexQueryException(
            String.format("Get a past range: [%d,%d]", usableRange[i], usableRange[i + 1]));
      } else if (usableRange[i] < usableRange[i + 1] - 1) {
        throw new IndexQueryException(
            String.format("Error range pair: [%d,%d]", usableRange[i], usableRange[i + 1]));
      }
      // [p, p-1] means no range
      else if (usableRange[i] == usableRange[i + 1] - 1) {
        // do nothing
      }
      // It's continue, merge them
      else if (usableRange[i] == lastTime + 1) {
        times.add(times.size() - 1, usableRange[i + 1]);
      } else {
        times.add(usableRange[i]);
        times.add(usableRange[i + 1]);
      }
    }
  }


  public void updateUsableRange(TimeRangeOld usableRangeInCurrentChunk) {
    throw new UnsupportedOperationException();
  }
}
