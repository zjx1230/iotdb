/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.index.algorithm.mmhh;

import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.RTreeIndex;
import org.apache.iotdb.db.index.algorithm.rtree.RTree.DistSeriesComparator;
import org.apache.iotdb.db.index.common.DistSeries;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.feature.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.stats.IndexStatManager;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import ai.djl.MalformedModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.function.Function;

import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_HASH_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_SERIES_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.HASH_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.MODEL_PATH;
import static org.apache.iotdb.db.index.common.IndexConstant.NO_PRUNE;
import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.SERIES_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.TOP_K;
import static org.apache.iotdb.db.index.common.IndexType.MMHH;

public class MMHHIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(MMHHIndex.class);

  private final File featureFile;
  private final String modelPath;
  private final int inputLength;
  private final int hashLength;
  private int itemSize;
  private HashMap<Long, List<Long>> hashLookupTable;
  private MMHHWholeFeatureExtractor mmhhFeatureExtractor;
  private PartialPath currentInsertPath;

  public MMHHIndex(PartialPath path, TSDataType tsDataType, String indexDir, IndexInfo indexInfo) {
    super(path, tsDataType, indexInfo);
    itemSize = 0;
    featureFile = IndexUtils.getIndexFile(indexDir + File.separator + "mmhhLookup");
    if (props.containsKey(MODEL_PATH)) {
      this.modelPath = props.get(MODEL_PATH);
    } else {
      throw new IllegalIndexParamException(MODEL_PATH + " is necessary for MMHH");
    }
    this.inputLength = Integer.parseInt(props.getOrDefault(SERIES_LENGTH, DEFAULT_SERIES_LENGTH));

    this.hashLength = Integer.parseInt(props.getOrDefault(HASH_LENGTH, DEFAULT_HASH_LENGTH));

    File indexDirFile = IndexUtils.getIndexFile(indexDir);
    if (indexDirFile.exists()) {
      logger.info("reload index {} from {}", MMHH, indexDir);
      deserializeHashLookup();
    } else {
      indexDirFile.mkdirs();
      hashLookupTable = new HashMap<>();
    }
  }

  @Override
  public void initFeatureExtractor(ByteBuffer previous, boolean inQueryMode) {
    if (this.indexFeatureExtractor != null) {
      try {
        this.indexFeatureExtractor.closeAndRelease();
      } catch (IOException e) {
        logger.warn("meet exceptions when releasing the previous extractor", e);
      }
    }
    try {
      this.mmhhFeatureExtractor = new MMHHWholeFeatureExtractor(modelPath, inputLength, hashLength);
    } catch (IOException | MalformedModelException e) {
      logger.error("meet errors when init feature extractor", e);
    }
    this.indexFeatureExtractor = mmhhFeatureExtractor;
  }

  /**
   * should be concise into WholeIndex or IoTDBIndex, it's duplicate
   *
   * @param tvList tvList to insert
   */
  public IndexFeatureExtractor startFlushTask(PartialPath partialPath, TVList tvList) {
    IndexFeatureExtractor res = super.startFlushTask(partialPath, tvList);
    currentInsertPath = partialPath;
    return res;
  }

  /** should be concise into WholeIndex or IoTDBIndex, it's duplicate */
  public void endFlushTask() {
    super.endFlushTask();
    currentInsertPath = null;
  }

  @Override
  public boolean buildNext() throws IndexManagerException {
    Long key = mmhhFeatureExtractor.getCurrent_L3_Feature();
    /** TODO it's just a trick! */
    Long pathId = Long.valueOf(currentInsertPath.getNodes()[currentInsertPath.getNodeLength() - 2]);
    List<Long> bucket = hashLookupTable.computeIfAbsent(key, id -> new ArrayList<>());
    bucket.add(pathId);
    itemSize++;
    //    System.out.println(String
    //        .format("Input record: %s, pathId %d, hash %d, series: %s", currentInsertPath, pathId,
    // key,
    //            mmhhFeatureExtractor.getCurrent_L2_AlignedSequence()));
    return true;
  }

  @Override
  protected void flushIndex() {
    serializeHashLookup();
    hashLookupTable.clear();
  }

  private void deserializeHashLookup() {
    if (!featureFile.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(featureFile)) {
      hashLookupTable = new HashMap<>();
      int tableSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < tableSize; i++) {
        Long key = ReadWriteIOUtils.readLong(inputStream);
        int bucketSize = ReadWriteIOUtils.readInt(inputStream);
        List<Long> bucket = new ArrayList<>(bucketSize);
        for (int j = 0; j < bucketSize; j++) {
          Long v = ReadWriteIOUtils.readLong(inputStream);
          bucket.add(v);
          itemSize++;
        }
        hashLookupTable.put(key, bucket);
      }
    } catch (IOException e) {
      logger.error("Error when deserialize ELB features. Given up.", e);
    }
  }

  private void serializeHashLookup() {
    try (OutputStream outputStream = new FileOutputStream(featureFile)) {
      //      ReadWriteIOUtils.write(modelPath, outputStream);
      //      ReadWriteIOUtils.write(inputLength, outputStream);
      //      ReadWriteIOUtils.write(hashLength, outputStream);
      ReadWriteIOUtils.write(hashLookupTable.size(), outputStream);
      for (Entry<Long, List<Long>> entry : hashLookupTable.entrySet()) {
        Long k = entry.getKey();
        List<Long> bucket = entry.getValue();
        ReadWriteIOUtils.write(k, outputStream);
        ReadWriteIOUtils.write(bucket.size(), outputStream);
        for (Long v : bucket) {
          ReadWriteIOUtils.write(v, outputStream);
        }
      }
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  private static class MMHHQueryStruct {

    /** features is represented by float array */
    //    float[] patternFeatures;
    //    TriFunction<float[], float[], float[], Double> calcLowerDistFunc;
    //
    //    BiFunction<double[], TVList, Double> calcExactDistFunc;
    //    Function<PartialPath, TVList> loadSeriesFunc;
    private double[] patterns;

    int topK = -1;
    double threshold = -1;
  }

  public MMHHQueryStruct initQuery(Map<String, Object> queryProps) {
    MMHHQueryStruct struct = new MMHHQueryStruct();

    if (queryProps.containsKey(TOP_K)) {
      struct.topK = (int) queryProps.get(TOP_K);
    } else {
      throw new IllegalIndexParamException("missing parameter: " + TOP_K);
    }
    if (queryProps.containsKey(PATTERN)) {
      struct.patterns = (double[]) queryProps.get(PATTERN);
    } else {
      throw new IllegalIndexParamException("missing parameter: " + PATTERN);
    }
    return struct;
  }

  @TestOnly
  public QueryDataSet noPruneQuery(
      Map<String, Object> queryProps,
      IIndexUsable iIndexUsable,
      QueryContext context,
      IIndexCandidateOrderOptimize candidateOrderOptimize,
      boolean alignedByTime)
      throws QueryIndexException {
    MMHHQueryStruct struct = initQuery(queryProps);
    long featureStart;
    featureStart = System.nanoTime();
    Long queryCode = mmhhFeatureExtractor.processQuery(struct.patterns);
    IndexStatManager.featureExtractCost += System.nanoTime() - featureStart;

    List<DistSeries> res;
    Function<PartialPath, TVList> loadSeriesFunc =
        RTreeIndex.getLoadSeriesFunc(context, tsDataType, mmhhFeatureExtractor);
    List<PartialPath> paths;
    try {
      Pair<List<PartialPath>, Integer> pathsPair =
          MManager.getInstance().getAllTimeseriesPathWithAlias(indexSeries, -1, -1);
      paths = pathsPair.left;
    } catch (MetadataException e) {
      e.printStackTrace();
      return null;
    }

    PriorityQueue<DistSeries> topKPQ = new PriorityQueue<>(struct.topK, new DistSeriesComparator());

    double kthMinDist = Double.MAX_VALUE;
    for (PartialPath path : paths) {
      TVList srcData = loadSeriesFunc.apply(path);
      double[] inputArray = new double[srcData.size()];
      //    featureArray
      for (int i = 0; i < inputArray.length; i++) {
        if (i >= srcData.size()) {
          inputArray[i] = 0;
          continue;
        }
        switch (srcData.getDataType()) {
          case INT32:
            inputArray[i] = srcData.getInt(i);
            break;
          case INT64:
            inputArray[i] = srcData.getLong(i);
            break;
          case FLOAT:
            inputArray[i] = srcData.getFloat(i);
            break;
          case DOUBLE:
            inputArray[i] = (float) srcData.getDouble(i);
            break;
          default:
            throw new NotImplementedException(srcData.getDataType().toString());
        }
      }
      featureStart = System.nanoTime();
      Long hashCode = mmhhFeatureExtractor.processQuery(inputArray);
      IndexStatManager.featureExtractCost += System.nanoTime() - featureStart;
      int tempDist = Long.bitCount(hashCode ^ queryCode);

      if (topKPQ.size() < struct.topK || tempDist < kthMinDist) {
        if (topKPQ.size() == struct.topK) {
          topKPQ.poll();
        }
        topKPQ.add(new DistSeries(tempDist, srcData, path));
        kthMinDist = topKPQ.peek().dist;
      }
    }

    if (topKPQ.isEmpty()) {
      res = Collections.emptyList();
    } else {
      int retSize = Math.min(struct.topK, topKPQ.size());
      DistSeries[] resArray = new DistSeries[retSize];
      int idx = retSize - 1;
      while (!topKPQ.isEmpty()) {
        DistSeries distSeries = topKPQ.poll();
        resArray[idx--] = distSeries;
      }
      res = Arrays.asList(resArray);
    }

    for (DistSeries ds : res) {
      ds.partialPath = ds.partialPath.concatNode(String.format("(NHam=%.2f)", ds.dist));
    }
    return constructSearchDataset(res, alignedByTime);
  }

  @Override
  public QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable iIndexUsable,
      QueryContext context,
      IIndexCandidateOrderOptimize candidateOrderOptimize,
      boolean alignedByTime)
      throws QueryIndexException {
    if (props.containsKey(NO_PRUNE)) {
      return noPruneQuery(queryProps, iIndexUsable, context, candidateOrderOptimize, alignedByTime);
    }

    MMHHQueryStruct struct = initQuery(queryProps);
    long featureStart = System.nanoTime();
    Long queryCode = mmhhFeatureExtractor.processQuery(struct.patterns);
    IndexStatManager.featureExtractCost += System.nanoTime() - featureStart;
    List<DistSeries> res = hammingSearch(queryCode, struct.topK, context);
    for (DistSeries ds : res) {
      ds.partialPath = ds.partialPath.concatNode(String.format("(D=%.2f)", ds.dist));
    }
    return constructSearchDataset(res, alignedByTime);
  }

  private List<DistSeries> hammingSearch(Long queryCode, int topK, QueryContext context) {
    //    System.out.println(String.format("query: %d, %s", queryCode,
    // Long.toBinaryString(queryCode)));
    List<DistSeries> res = new ArrayList<>();
    Function<PartialPath, TVList> loadRaw =
        RTreeIndex.getLoadSeriesFunc(context, tsDataType, mmhhFeatureExtractor);
    for (int radius = 0; radius <= hashLength; radius++) {
      boolean full = scanBucket(queryCode, 0, radius, 0, topK, loadRaw, res);
      if (full) {
        break;
      }
    }
    return res;
  }

  /** if res has reached topK */
  private boolean scanBucket(
      long queryCode,
      int doneIdx,
      int maxIdx,
      int startIdx,
      int topK,
      Function<PartialPath, TVList> loadSeriesFunc,
      List<DistSeries> res) {
    if (doneIdx == maxIdx) {
      if (hashLookupTable.containsKey(queryCode)) {
        //        System.out.println(String.format("found: %d, %s, ham dist=%d",
        //            queryCode, Long.toBinaryString(queryCode), maxIdx));
        List<Long> bucket = hashLookupTable.get(queryCode);
        for (Long seriesId : bucket) {
          res.add(readRawData(maxIdx, seriesId, loadSeriesFunc));
          if (res.size() == topK) {
            return true;
          }
        }
      }
    } else {
      for (int doIdx = startIdx; doIdx <= hashLength - (maxIdx - doneIdx); doIdx++) {
        // change bit
        queryCode = reverseBit(queryCode, doIdx);
        boolean full =
            scanBucket(queryCode, doneIdx + 1, maxIdx, doIdx + 1, topK, loadSeriesFunc, res);
        if (full) {
          return true;
        }
        // change bit back
        queryCode = reverseBit(queryCode, doIdx);
      }
    }
    return false;
  }

  private long reverseBit(long hashCode, int idx) {
    long flag = 1L << idx;
    if ((hashCode & flag) != 0) {
      // the idx-th bit: 1 to 0
      return hashCode & ~flag;
    } else {
      // the idx-th bit: 0 to 1
      return hashCode | flag;
    }
  }

  private DistSeries readRawData(
      int hammingDist, Long seriesId, Function<PartialPath, TVList> loadSeriesFunc) {
    int len = indexSeries.getNodeLength();
    String[] nodes = Arrays.copyOf(indexSeries.getNodes(), len);
    nodes[len - 2] = seriesId.toString();
    PartialPath path = new PartialPath(nodes);
    TVList rawData = loadSeriesFunc.apply(path);
    return new DistSeries(hammingDist, rawData, path);
  }

  @Override
  public String toString() {
    return String.format(
        "{#bucket=%d, #size=%d, table=%s}",
        hashLookupTable.size(), itemSize, hashLookupTable.toString());
  }
}
