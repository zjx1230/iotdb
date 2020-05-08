package org.apache.iotdb.db.index.read.optimize;

import org.apache.iotdb.db.index.read.IndexTimeRange;

public class NaiveOptimizer implements IndexQueryOptimize {

  @Override
  public boolean needUnpackIndexChunk(IndexTimeRange indexUsableRange,
      long indexChunkStart, long indexChunkEnd) {
    return true;
  }
}
