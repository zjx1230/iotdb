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
package org.apache.iotdb.db.index.algorithm.elb;

import static org.apache.iotdb.db.index.common.IndexConstant.BORDER;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.FEATURE_DIM;
import static org.apache.iotdb.db.index.common.IndexConstant.MISSING_PARAM_ERROR_MESSAGE;
import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBWindowBlockFeature;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.func.IndexFuncFactory;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Use ELB to represent a pattern, and slide the pattern over a long time series to find
 * sliding windows whose distance is less than the given threshold. Considering the original setting
 * in the paper, the sliding step is limited to 1. We will extend the work to the case of arbitrary
 * sliding step in the future.
 * </p>
 *
 * <p>Parameters for Creating ELB-Match:
 * Window Range,
 * </p>
 *
 * <p>Parameters for Querying ELB-Match</p>
 *
 *
 * <p>Query Parameters:</p>
 * <ul>
 *   <li>PATTERN: pattern series,</li>
 *   <li>THRESHOLD: [eps_1, eps_2, ..., eps_b];</li>
 *   <li>BORDER: [left_1, left_2, ..., left_b]; where left_1 is always 0</li>
 * </ul>
 *
 * <p>
 * The above borders indicate the subpattern borders.
 * For example, the range of the i-th subpattern is [left_i, left_{i+1}) with threshold eps_i.
 * </p>
 */

public class ELBIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(ELBIndex.class);
  private Distance distance;
  private ELBType elbType;

  private ELBMatchFeatureExtractor elbMatchPreprocessor;

  // Only for query
  private int featureDim;
  private double[] pattern;
  // leaf: upper bounds, right: lower bounds
  private Pair<double[], double[]> patternFeatures;
  private ELBFeatureExtractor elbFeatureExtractor;

  public ELBIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo);
    initELBParam();
  }

  private void initELBParam() {
    this.distance = Distance.getDistance(props.getOrDefault(DISTANCE, DEFAULT_DISTANCE));
    elbType = ELBType.valueOf(props.getOrDefault(ELB_TYPE, DEFAULT_ELB_TYPE));
    this.featureDim = Integer.parseInt(props.get(FEATURE_DIM));
  }

  @Override
  public void initPreprocessor(ByteBuffer previous, boolean inQueryMode) {
    if (this.indexFeatureExtractor != null) {
      this.indexFeatureExtractor.clear();
    }
    this.elbMatchPreprocessor = new ELBMatchFeatureExtractor(tsDataType, windowRange, featureDim,
        elbType);
    this.indexFeatureExtractor = elbMatchPreprocessor;
    elbMatchPreprocessor.setInQueryMode(inQueryMode);
    indexFeatureExtractor.deserializePrevious(previous);
  }

  @Override
  public boolean buildNext() {
    return true;
  }

  @Override
  public IndexFlushChunk flush() {
    if (indexFeatureExtractor.getCurrentChunkSize() == 0) {
      logger.warn("Nothing to be flushed, directly return null");
      System.out.println("Nothing to be flushed, directly return null");
      return null;
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // serialize window block features
    try {
      elbMatchPreprocessor.serializeFeatures(outputStream);
    } catch (IOException e) {
      logger.error("flush failed", e);
      return null;
    }
    long st = indexFeatureExtractor.getChunkStartTime();
    long end = indexFeatureExtractor.getChunkEndTime();
    return new IndexFlushChunk(path, indexType, outputStream, st, end);
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException();
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
    if (queryConditions.containsKey(PATTERN)) {
      this.pattern = (double[]) queryConditions.get(PATTERN);
    } else {
      throw new UnsupportedIndexFuncException(String.format(MISSING_PARAM_ERROR_MESSAGE, PATTERN));
    }
    // calculate ELB upper/lower bounds of the given pattern according to given segmentation and threshold.
    double[] thresholds;
    if (queryConditions.containsKey(THRESHOLD)) {
      thresholds = (double[]) queryConditions.get(THRESHOLD);
    } else {
      throw new UnsupportedIndexFuncException(
          String.format(MISSING_PARAM_ERROR_MESSAGE, THRESHOLD));
    }

    int[] borders;
    if (queryConditions.containsKey(BORDER)) {
      borders = (int[]) queryConditions.get(BORDER);
    } else {
      throw new UnsupportedIndexFuncException(String.format(MISSING_PARAM_ERROR_MESSAGE, BORDER));
    }
    calculatePatternFeatures(thresholds, borders);
  }

  private void calculatePatternFeatures(double[] thresholds, int[] borders) {
    // calculate pattern features
    //TODO note that, the primary ELB version adopts an inelegant implementation for overcritical
    // memory control. A decoupling reconstruction will be done afterwards.
    // Convert the pattern array into TVList format is mere temporary patch. It will be modified soon.
    TVList patternList = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
    for (double v : pattern) {
      patternList.putDouble(0, v);
    }
    this.elbFeatureExtractor = new ELBFeatureExtractor(distance, windowRange,
        featureDim, elbType);
    this.patternFeatures = elbFeatureExtractor
        .calcELBFeature(patternList, 0, thresholds, borders);
  }

  @Override
  public List<Identifier> queryByIndex(ByteBuffer indexChunkData) throws IndexManagerException {
    // deserialize
    List<ELBWindowBlockFeature> windowBlockFeatures;
    try {
      windowBlockFeatures = ELBMatchFeatureExtractor.deserializeFeatures(indexChunkData);
    } catch (IOException e) {
      throw new IndexManagerException(e.getMessage());
    }

    List<Identifier> res = new ArrayList<>();
    // pruning
    for (int i = 0; i <= windowBlockFeatures.size() - featureDim; i++) {
      boolean canBePruned = false;
      for (int j = 0; j < featureDim; j++) {
        if (patternFeatures.left[j] < windowBlockFeatures.get(i + j).feature ||
            patternFeatures.right[j] > windowBlockFeatures.get(i + j).feature) {
          canBePruned = true;
          break;
        }
      }
      if (!canBePruned) {
        res.add(new Identifier(windowBlockFeatures.get(i).startTime,
            windowBlockFeatures.get(i).endTime,
            -1));
      }
    }
    return res;
  }

  @Override
  public int postProcessNext(List<IndexFuncResult> funcResult) throws QueryIndexException {
    TVList aligned = (TVList) indexFeatureExtractor.getCurrent_L2_AlignedSequence();
    int reminding = funcResult.size();
    if ( elbFeatureExtractor.exactDistanceCalc(aligned)){
      for (IndexFuncResult result : funcResult) {
        IndexFuncFactory.basicSimilarityCalc(result, indexFeatureExtractor, pattern);
      }
    }
    TVListAllocator.getInstance().release(aligned);
    return reminding;
  }
}
