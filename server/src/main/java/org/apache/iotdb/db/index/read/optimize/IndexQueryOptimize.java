/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
