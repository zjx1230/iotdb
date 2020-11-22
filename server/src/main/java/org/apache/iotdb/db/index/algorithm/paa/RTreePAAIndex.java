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
package org.apache.iotdb.db.index.algorithm.paa;

import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.algorithm.RTreeIndex;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.func.IndexFuncFactory;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
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
 * <p>Use PAA as the feature of MBRIndex.</p>
 */

public class RTreePAAIndex extends RTreeIndex {

  private static final Logger logger = LoggerFactory.getLogger(RTreePAAIndex.class);

  private PAATimeFixedFeatureExtractor paaTimeFixedPreprocessor;

  // Only for query
  private Map<Integer, Identifier> identifierMap = new HashMap<>();

  public RTreePAAIndex(PartialPath path,
      TSDataType tsDataType, String indexDir,
      IndexInfo indexInfo) {
    super(path, tsDataType, indexInfo, true);
    IndexUtils.breakDown("indexDir没用起来，记得初始化");
  }

  @Override
  public void initPreprocessor(ByteBuffer previous, boolean inQueryMode) {
    if (this.indexFeatureExtractor != null) {
      this.indexFeatureExtractor.clear();
    }
    this.paaTimeFixedPreprocessor = new PAATimeFixedFeatureExtractor(tsDataType, windowRange, slideStep,
        featureDim, confIndexStartTime, true, false);
    paaTimeFixedPreprocessor.deserializePrevious(previous);
    this.indexFeatureExtractor = paaTimeFixedPreprocessor;
  }

  @Override
  public QueryDataSet query(Map<String, Object> queryProps, IIndexUsable iIndexUsable,
      QueryContext context, IIndexRefinePhaseOptimize refinePhaseOptimizer, boolean alignedByTime)
      throws QueryIndexException {
    return null;
  }


  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  @Override
  protected int fillCurrentFeature() {
    paaTimeFixedPreprocessor.copyFeature(currentLowerBounds);
    System.arraycopy(currentLowerBounds,0,currentUpperBounds,0,currentLowerBounds.length);
    return paaTimeFixedPreprocessor.getSliceNum() - 1;
  }

  @Override
  protected BiConsumer<Integer, OutputStream> getSerializeFunc() {
    return (idx, outputStream) -> {
      try {
        paaTimeFixedPreprocessor.serializeIdentifier(idx, outputStream);
      } catch (IOException e) {
        logger.error("serialized error.", e);
      }
    };
  }


  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }

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
  protected BiConsumer<Integer, ByteBuffer> getDeserializeFunc() {
    return (idx, input) -> {
      Identifier identifier = Identifier.deserialize(input);
      identifierMap.put(idx, identifier);
    };
  }

  @Override
  protected List<Identifier> getQueryCandidates(List<Integer> candidateIds) {
    List<Identifier> res = new ArrayList<>(candidateIds.size());
    candidateIds.forEach(i->res.add(identifierMap.get(i)));
    this.identifierMap.clear();
    return res;
  }


  /**
   * PAA has lower bounding property.
   */
  @Override
  protected double calcLowerBoundThreshold(double queryThreshold) {
    return queryThreshold;
  }

  @Override
  protected void calcAndFillQueryFeature() {
    Arrays.fill(currentLowerBounds, 0);
    Arrays.fill(currentUpperBounds, 0);
    int intervalWidth = windowRange / featureDim;
    for (int i = 0; i < featureDim; i++) {
      for (int j = 0; j < intervalWidth; j++) {
        currentLowerBounds[i] += patterns[i * intervalWidth + j];
      }
      currentLowerBounds[i] /= intervalWidth;
      currentUpperBounds[i] /= currentLowerBounds[i];
    }
  }

  public int postProcessNext(List<IndexFuncResult> funcResult) throws QueryIndexException {
    Identifier identifier =  indexFeatureExtractor.getCurrent_L1_Identifier();
    TVList srcList = indexFeatureExtractor
        .get_L0_SourceData(identifier.getStartTime(), identifier.getEndTime());
    TVList aligned = IndexUtils.alignUniform(srcList, patterns.length);
    double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
    System.out.println(String.format(
        "PAA Process: ed:%.3f: %s", ed, IndexUtils.tvListToStr(aligned)));
    int reminding = funcResult.size();
    if (ed <= threshold) {
      for (IndexFuncResult result : funcResult) {
        if (result.getIndexFunc() == IndexFunc.ED) {
          result.addScalar(ed);
        } else {
          IndexFuncFactory.basicSimilarityCalc(result, indexFeatureExtractor, patterns);
        }
      }
    }
    TVListAllocator.getInstance().release(aligned);

    return reminding;
  }
}
