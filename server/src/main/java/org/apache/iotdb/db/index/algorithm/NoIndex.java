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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.CountFixedFeatureExtractor;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.func.IndexFuncFactory;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
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

  public NoIndex(String path, String indexDir, IndexInfo indexInfo) {
    super(path, indexInfo);
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
        slideStep, false, false);
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
  @Override
  public List<Identifier> queryByIndex(ByteBuffer indexChunkData) throws IndexManagerException {
    // return null directly
    throw new UnsupportedOperationException("NoIndex ,query ,return what?");
  }


  @Override
  public void initQuery(Map<String, Object> queryConditions, List<IndexFuncResult> indexFuncResults)
      throws UnsupportedIndexFuncException {
    for (IndexFuncResult result : indexFuncResults) {
      switch (result.getIndexFunc()) {
        case TIME_RANGE:
        case SIM_ST:
        case SIM_ET:
        case SERIES_LEN:
        case ED:
        case DTW:
          result.setIsTensor(true);
          break;
        default:
          throw new UnsupportedIndexFuncException(indexFuncResults.toString());
      }
      result.setIndexFuncDataType(result.getIndexFunc().getType());
    }
    if (queryConditions.containsKey(THRESHOLD)) {
      this.threshold = (double) queryConditions.get(THRESHOLD);
    } else {
      this.threshold = Double.MAX_VALUE;
    }
    if (queryConditions.containsKey(PATTERN)) {
      this.patterns = (double[]) queryConditions.get(PATTERN);
    } else {
      throw new UnsupportedIndexFuncException("missing parameter: " + PATTERN);
    }
  }

  @Override
  public int postProcessNext(List<IndexFuncResult> funcResult) throws QueryIndexException {
    TVList aligned = (TVList) indexFeatureExtractor.getCurrent_L2_AlignedSequence();
    double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
    System.out.println(String.format(
        "NoIndex Process: ed:%.3f: %s", ed, IndexUtils.tvListToStr(aligned)));
    int reminding = funcResult.size();
    if (ed <= threshold) {
      for (IndexFuncResult result : funcResult) {
        IndexFuncFactory.basicSimilarityCalc(result, indexFeatureExtractor, patterns);
      }
    }
    TVListAllocator.getInstance().release(aligned);
    return reminding;
  }

  /**
   * All it needs depends on its preprocessor. Just for explain.
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public int getAmortizedSize() {
    return super.getAmortizedSize();
  }


}
