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
package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.db.index.common.IndexConstant.NON_IMPLEMENTED_MSG;

import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * For all indexes, the raw input sequence has to be pre-processed before it's organized by indexes.
 * In general, index structure needn't maintain all of original data, but only pointers (e.g. the
 * identifier [start_time, end_time, series_path] can identify a time sequence uniquely).
 *
 * By and large, similarity index supposes the input data are ideal: fixed dimension, equal
 * interval, not missing values and even not outliers. However, in real scenario, the input series
 * may contain missing values (thus it's not dimension-fixed) and the point's timestamp may contain
 * slight offset (thus they are not equal-interval). IndexFeatureExtractor need to preprocess the
 * series and obtain clean series to insert.
 *
 * Many indexes will further extract features of alignment sequences, such as PAA, SAX, FFT, etc.
 *
 * In summary, the IndexFeatureExtractor can provide three-level features:
 *
 * <ul>
 *   <li>L1: a triplet to identify a series: {@code {StartTime, EndTime, Length}} (not submitted in this pr)
 *   <li>L2: aligned sequence: {@code {a1, a2, ..., an}}
 *   <li>L3: feature: {@code {C1, C2, ..., Cm}}
 * </ul>
 */
public abstract class IndexFeatureExtractor {

  /**
   * In the BUILD and QUERY modes, the IndexFeatureExtractor may work differently.
   */
  protected boolean inQueryMode;

  public IndexFeatureExtractor(boolean inQueryMode) {
    this.inQueryMode = inQueryMode;
  }

  /**
   * Input a list of new data into the FeatureProcessor.
   *
   * @param newData new coming data.
   */
  public abstract void appendNewSrcData(TVList newData);

  /**
   * Input a list of new data into the FeatureProcessor. Due to the exist codes, IoTDB generate
   * TVList in the insert phase, but obtain BatchData in the query phase.
   *
   * @param newData new coming data.
   */
  public abstract void appendNewSrcData(BatchData newData);

  /**
   * Having done {@code appendNewSrcData}, the index framework will check {@code hasNext}.
   */
  public abstract boolean hasNext();

  /**
   * If {@code hasNext} returns true, the index framework will call {@code processNext} which
   * prepares a new series item (ready for insert).
   */
  public abstract void processNext();

  /**
   * After processing a batch of data, the index framework may call {@code clearProcessedSrcData} to
   * clean out the processed data for releasing memory.
   */
  public abstract void clearProcessedSrcData();

  /**
   * Called when the memory reaches the threshold. This function should release all allocated array
   * list which increases with the number of processed data pairs.
   *
   * <p>Note that, after cleaning up all past store, the next {@linkplain #processNext()} will
   * still start from the current point.
   *
   * <p>IndexPreprocessor releases {@code previous} but <tt>doesn't release {@code srcData}</tt>
   * which may be still usable for next chunks, it's an important difference from {@code
   * releaseSrcData}.
   *
   * <p>We do not call {@linkplain #clearProcessedSrcData} when triggering Sub-Flush, but use
   * offset to label how many point we have processed, because Sub-Flush may be triggered frequently
   * when the memory threshold is relatively small. If we use {@linkplain #clearProcessedSrcData},
   * we need to move unprocessed data to position 0. The less data we flush each time, the more data
   * we need to move. Therefore, we only use appendSrcData when starting startFlushMemTable.
   */
  public abstract long clear();

  public abstract ByteBuffer closeAndRelease() throws IOException;

  /**
   * get current L2 aligned sequences. The caller needs to release them after use.
   */
  public Object getCurrent_L2_AlignedSequence() {
    throw new UnsupportedOperationException(NON_IMPLEMENTED_MSG);
  }

  public Object getCurrent_L3_Feature() {
    throw new UnsupportedOperationException(NON_IMPLEMENTED_MSG);
  }
}
