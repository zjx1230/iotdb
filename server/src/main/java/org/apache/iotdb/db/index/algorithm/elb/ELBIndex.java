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

import static org.apache.iotdb.db.index.common.IndexConstant.BLOCK_SIZE;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_BLOCK_SIZE;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.MISSING_PARAM_ERROR_MESSAGE;
import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELB.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.ELB.ELBWindowBlockFeature;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.read.TVListPointer;
import org.apache.iotdb.db.index.read.optimize.IIndexRefinePhaseOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
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

  private final Logger logger = LoggerFactory.getLogger(ELBIndex.class);
  private ELBType elbType;

  private ELBMatchFeatureExtractor elbMatchPreprocessor;
  private List<ELBWindowBlockFeature> windowBlockFeatures;
  private PrimitiveList usableBlocks;

  private int blockWidth;
  private File featureFile;

  public ELBIndex(PartialPath path, TSDataType tsDataType,
      String indexDir, IndexInfo indexInfo) {
    super(path, tsDataType, indexInfo);
    windowBlockFeatures = new ArrayList<>();
    featureFile = IndexUtils.getIndexFile(indexDir + File.separator + "feature");
    File indexDirFile = IndexUtils.getIndexFile(indexDir);
    if (indexDirFile.exists()) {
      System.out.println(String.format("reload index %s from %s", ELB_INDEX, indexDir));
      deserializeFeatures();
    } else {
      indexDirFile.mkdirs();
    }
//    logger.debug("");
    // ELB always variable query length, so it's needed windowRange
    windowRange = -1;
    usableBlocks = PrimitiveList.newList(TSDataType.BOOLEAN);
    initELBParam();
//    throw new IndexRuntimeException("indexDir没用起来，记得初始化");
  }

  private void initELBParam() {
    elbType = ELBType.valueOf(props.getOrDefault(ELB_TYPE, DEFAULT_ELB_TYPE));
    this.blockWidth = props.containsKey(BLOCK_SIZE) ? Integer.parseInt(props.get(BLOCK_SIZE))
        : DEFAULT_BLOCK_SIZE;
  }

  @Override
  public void initPreprocessor(ByteBuffer previous, boolean inQueryMode) {
    if (this.indexFeatureExtractor != null) {
      this.indexFeatureExtractor.clear();
    }
    this.elbMatchPreprocessor = new ELBMatchFeatureExtractor(tsDataType, windowRange, blockWidth,
        elbType, inQueryMode);
    this.indexFeatureExtractor = elbMatchPreprocessor;
    indexFeatureExtractor.deserializePrevious(previous);
  }

  @Override
  public boolean buildNext() {
    ELBWindowBlockFeature block = (ELBWindowBlockFeature) elbMatchPreprocessor
        .getCurrent_L3_Feature();
    windowBlockFeatures.add(block);
    return true;
  }

  @Override
  public void flush() {
    // we need to do nothing when a batch of memtable flush out.
//    if (indexFeatureExtractor.getCurrentChunkSize() == 0) {
//      logger.warn("Nothing to be flushed, directly return null");
//      System.out.println("Nothing to be flushed, directly return null");
//      return;
//    }
//    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//    // serialize window block features
//    try {
//      elbMatchPreprocessor.serializeFeatures(outputStream);
//    } catch (IOException e) {
//      logger.error("flush failed", e);
//      return;
//    }
//    long st = indexFeatureExtractor.getChunkStartTime();
//    long end = indexFeatureExtractor.getChunkEndTime();
//    return new IndexFlushChunk(path, indexType, outputStream, st, end);
//    elbMatchPreprocessor.serializeFeatures(outputStream);
  }


  private void deserializeFeatures() {
    if (!featureFile.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(featureFile)) {
      int size = ReadWriteIOUtils.readInt(inputStream);

      for (int i = 0; i < size; i++) {
        long startTime = ReadWriteIOUtils.readLong(inputStream);
        long endTime = ReadWriteIOUtils.readLong(inputStream);
        double feature = ReadWriteIOUtils.readDouble(inputStream);
        windowBlockFeatures.add(new ELBWindowBlockFeature(startTime, endTime, feature));
      }
    } catch (IOException e) {
      logger.error("Error when deserialize ELB features. Given up.", e);
    }

  }

  @Override
  protected void serializeIndexAndFlush() {
    try (OutputStream outputStream = new FileOutputStream(featureFile)) {
      ReadWriteIOUtils.write(windowBlockFeatures.size(), outputStream);
      for (ELBWindowBlockFeature features : windowBlockFeatures) {
        ReadWriteIOUtils.write(features.startTime, outputStream);
        ReadWriteIOUtils.write(features.endTime, outputStream);
        ReadWriteIOUtils.write(features.feature, outputStream);
      }
      usableBlocks.clearAndRelease();
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }


  @Override
  public QueryDataSet query(Map<String, Object> queryProps, IIndexUsable iIndexUsable,
      QueryContext context, IIndexRefinePhaseOptimize refinePhaseOptimizer, boolean alignedByTime)
      throws QueryIndexException {
    ELBQueryStruct struct = new ELBQueryStruct();

    initQuery(struct, queryProps);
//    for (double v : struct.pattern) {
//      System.out.println(String.format("%.3f", v));
//    }
    List<Filter> filterList = queryByIndex(struct, iIndexUsable);
    List<TVList> res = new ArrayList<>();
    try {
      for (Filter timeFilter : filterList) {
        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(indexSeries, context, timeFilter);
        timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

        IBatchReader reader = new SeriesRawDataBatchReader(indexSeries,
            Collections.singleton(indexSeries.getMeasurement()), tsDataType, context,
            queryDataSource, timeFilter, null, null, true);
        ELBMatchFeatureExtractor featureExtractor = new ELBMatchFeatureExtractor(tsDataType,
            struct.pattern.length, blockWidth, elbType, true);
//        System.out.println(">>>>>>>>>>>>>>>> Time range: " + timeFilter);
        while (reader.hasNextBatch()) {
          BatchData batch = reader.nextBatch();
          featureExtractor.appendNewSrcData(batch);
          while (featureExtractor.hasNext()) {
            featureExtractor.processNext();
            TVListPointer p = featureExtractor.getCurrent_L2_AlignedSequence();
//            System.out.println(">>>>>>>>>>>>>>>> " + IndexUtils.tvListToStr(p.tvList, p.offset, p.length));
            if (struct.elb.exactDistanceCalc(p.tvList, p.offset)) {
              TVList series = TVListAllocator.getInstance().allocate(tsDataType);
              TVList.append(series, p.tvList, p.offset, p.length);
              res.add(series);
            }
          }
          featureExtractor.clearProcessedSrcData();
        }
        reader.close();
      }
    } catch (StorageEngineException | QueryProcessException | IOException e) {
      throw new QueryIndexException(e.getMessage());
    }
    return constructSubMatchingDataset(indexSeries, res, alignedByTime, 5);
  }


  private static class ELBQueryStruct {

    double[] thresholds;
    int[] borders;
    private int blockNum;
    private double[] pattern;
    private Distance distance;
    // Only for query

    // leaf: upper bounds, right: lower bounds
    private Pair<double[], double[]> patternFeatures;
    private ELB elb;

  }

  //  @Deprecated
//  @Override
  @SuppressWarnings("unchecked")
  private void initQuery(ELBQueryStruct struct, Map<String, Object> queryProps) {
    struct.distance = Distance.getDistance(props.getOrDefault(DISTANCE, DEFAULT_DISTANCE));

    if (!queryProps.containsKey(THRESHOLD)) {
      throw new IllegalIndexParamException(
          String.format(MISSING_PARAM_ERROR_MESSAGE, THRESHOLD));
    }

    if (!queryProps.containsKey(PATTERN)) {
      throw new IllegalIndexParamException(String.format(MISSING_PARAM_ERROR_MESSAGE, PATTERN));
    }

    int patternLength = 0;
    List<double[]> patternList = (List<double[]>) queryProps.get(PATTERN);
    List<Double> thresholdList = (List<Double>) queryProps.get(THRESHOLD);

    struct.borders = new int[patternList.size() + 1];
    struct.thresholds = new double[patternList.size()];

    for (int i = 0; i < patternList.size(); i++) {
      double[] pattern = patternList.get(i);
      patternLength += pattern.length;
      struct.thresholds[i] = thresholdList.get(i);
      struct.borders[i + 1] = patternLength;

    }
    struct.pattern = new double[patternLength];
    int l = 0;
    for (double[] p : patternList) {
      System.arraycopy(p, 0, struct.pattern, l, p.length);
      l += p.length;
    }

    // calculate ELB upper/lower bounds of the given pattern according to given segmentation and threshold.

    struct.elb = new ELB(struct.distance, struct.pattern.length, blockWidth, elbType);
    struct.patternFeatures = struct.elb
        .calcELBFeature(struct.pattern, 0, struct.thresholds, struct.borders);
    struct.blockNum = struct.patternFeatures.left.length;
//    struct.featureExtractor = new ELBMatchFeatureExtractor(tsDataType, struct.pattern.length,
//        blockWidth, elbType, true);
  }


  private void refreshUnusableBlocks(IIndexUsable indexChanged) {
    indexChanged.updateELBBlocksForSeriesMatching(usableBlocks, windowBlockFeatures);
  }

  private List<Filter> queryByIndex(ELBQueryStruct struct, IIndexUsable indexChanged) {
    refreshUnusableBlocks(indexChanged);
    IIndexUsable cannotPruned = IIndexUsable.Factory.getIndexUsability(indexSeries);
    int wbfSize = windowBlockFeatures.size();
    if (wbfSize >= struct.blockNum) {
      // in the best cases, [wbf[0].start, wbf[wbfSize-featureDim].end] can be pruned
      cannotPruned.addUsableRange(indexSeries, windowBlockFeatures.get(0).startTime,
          windowBlockFeatures.get(wbfSize - struct.blockNum).endTime);
      // pruning
      for (int i = 0; i <= windowBlockFeatures.size() - struct.blockNum; i++) {
        boolean canBePruned = false;
        for (int j = 0; j < struct.blockNum; j++) {
          if (usableBlocks.getBoolean(i + j) &&
              (struct.patternFeatures.left[j] <= windowBlockFeatures.get(i + j).feature ||
                  struct.patternFeatures.right[j] >= windowBlockFeatures.get(i + j).feature)) {
            canBePruned = true;
            break;
          }
        }
        if (!canBePruned) {
          // update range
//        res.add(new Identifier(windowBlockFeatures.get(i).startTime,
//            windowBlockFeatures.get(i).endTime, -1));
          long startTime = windowBlockFeatures.get(i).startTime;
          int endIdx = i + struct.blockNum + (struct.pattern.length % blockWidth == 0 ? 0 : 1);
          long endTime = endIdx >= wbfSize ?
              Long.MAX_VALUE : windowBlockFeatures.get(endIdx).endTime;
          cannotPruned.minusUsableRange(indexSeries, startTime, endTime);

        }
      }
    }
    return indexChanged.getUnusableRangeForSeriesMatching(cannotPruned);
  }

  @Deprecated
//  @Override
//  private int postProcessNext(List<IndexFuncResult> funcResult) throws QueryIndexException {
//    TVList aligned = (TVList) indexFeatureExtractor.getCurrent_L2_AlignedSequence();
//    int reminding = funcResult.size();
////    if (elbFeatureExtractor.exactDistanceCalc(aligned)) {
////      for (IndexFuncResult result : funcResult) {
////        IndexFuncFactory.basicSimilarityCalc(result, indexFeatureExtractor, pattern);
////      }
////    }
//    TVListAllocator.getInstance().release(aligned);
//    return reminding;
//  }

  @Override
  public String toString() {
    return windowBlockFeatures.toString();
  }

}
