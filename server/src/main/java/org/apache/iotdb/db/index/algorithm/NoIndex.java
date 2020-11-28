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
package org.apache.iotdb.db.index.algorithm;

import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.preprocess.CountFixedFeatureExtractor;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.optimize.IIndexRefinePhaseOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NoIndex do nothing on feature extracting and data pruning. Its index-available range is always
 * empty.
 */

public class NoIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(NoIndex.class);
  private double[] patterns;
  private double threshold;

  public NoIndex(PartialPath path, TSDataType tsDataType,
      String indexDir, IndexInfo indexInfo) {
    super(path, tsDataType, indexInfo);
  }


  @Override
  public void initPreprocessor(ByteBuffer previous, boolean inQueryMode) {
    if (!inQueryMode) {
      return;
    }
    IndexUtils.breakDown();

    if (this.indexFeatureExtractor != null) {
      this.indexFeatureExtractor.clear();
    }
    this.indexFeatureExtractor = new CountFixedFeatureExtractor(tsDataType, windowRange,
        slideStep, false, false, inQueryMode);
    indexFeatureExtractor.deserializePrevious(previous);
  }

  @Override
  public boolean buildNext() {
    return true;
  }

  /**
   * convert the L1 identifiers to byteArray
   */
  @Override
  public void flush() {
  }

  /**
   * Nothing to be cleared, no more memory is released. Thus, we call the super method directly.
   * Just for explain.
   *
   * @return 0
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public long clearFeatureExtractor() {
    return super.clearFeatureExtractor();
  }

  @Override
  protected void serializeIndexAndFlush() {
    // do nothing
  }

  @Override
  public ByteBuffer serializeFeatureExtractor() {
    return ByteBuffer.allocate(0);
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }

  /**
   * NoIndex cannot prune anything. Return null.
   */
  public List<Identifier> queryByIndex(ByteBuffer indexChunkData) throws IndexManagerException {
    // return null directly
    throw new UnsupportedOperationException("NoIndex ,query ,return what?");
  }


  /**
   * All it needs depends on its preprocessor. Just for explain.
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public int getAmortizedSize() {
    return super.getAmortizedSize();
  }

  @Override
  public QueryDataSet query(Map<String, Object> queryProps, IIndexUsable iIndexUsable,
      QueryContext context, IIndexRefinePhaseOptimize refinePhaseOptimizer, boolean alignedByTime)
      throws QueryIndexException {
    return null;
  }


}
