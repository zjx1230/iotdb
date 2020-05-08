package org.apache.iotdb.db.index.read.optimize;

import org.apache.iotdb.db.index.read.IndexTimeRange;

public interface IndexQueryOptimize {

  /**
   * <p>Decide whether we need to unpack the index chunk, which is affected by many factors:</p>
   *
   * -The simplest strategy: load everything;
   *
   * -time range: if the data time range and index chunk range overlap too small, but the index
   * chunk itself is too large.
   *
   * -indexed data distribution: the time points covered by the index chunk may be uniform or not.
   *
   * -query cost: If the post-processing cost for one item is much higher than that of indexing.
   * it's worthy to unpack.
   *
   * -cache or not: if already cached, take it directly; if not, consider the amortization cost,
   * because it might be used later.
   *
   * -hit Rate: we will decrease the index score if its past hits are not good.
   */
  boolean needUnpackIndexChunk(IndexTimeRange indexUsableRange,
      long indexChunkStart, long indexChunkEnd);
}
