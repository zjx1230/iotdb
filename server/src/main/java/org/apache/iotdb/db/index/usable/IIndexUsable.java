package org.apache.iotdb.db.index.usable;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.read.IndexTimeRange;
import org.apache.iotdb.db.index.router.BasicIndexRouter;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.metadata.PartialPath;

public interface IIndexUsable {

  void addUsableRange(PartialPath partialPath, long startTime, long endTime);

  void addUnusableRange(PartialPath partialPath, long startTime, long endTime);

  public static class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexUsable getIndexUsability() {
      return new BasicIndexUsability(null);
    }
  }
}
