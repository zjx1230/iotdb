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
package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.algorithm.rtree.RTree;
import org.apache.iotdb.db.index.algorithm.rtree.RTree.SeedsPicker;
import org.apache.iotdb.db.index.common.DistSeries;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.TriFunction;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.index.usable.MultiShortIndexUsability;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_FEATURE_DIM;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_SERIES_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.FEATURE_DIM;
import static org.apache.iotdb.db.index.common.IndexConstant.MAX_ENTRIES;
import static org.apache.iotdb.db.index.common.IndexConstant.MIN_ENTRIES;
import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.SEED_PICKER;
import static org.apache.iotdb.db.index.common.IndexConstant.SERIES_LENGTH;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;
import static org.apache.iotdb.db.index.common.IndexConstant.TOP_K;
import static org.apache.iotdb.db.index.common.IndexType.RTREE_PAA;

/**
 * MBRIndex extracts features for input series data and put them into a root leaf node initially.
 * When the size of a leaf node achieve the specified threshold, the leaf node will split. MBRIndex
 * constructs hierarchical inner nodes above leaf nodes according to a lower bounding constraint for
 * guaranteeing no-false-dismissals pruning. For all indexes that applies MBR rules, serialization
 * and deserialization methods for features are required.
 */
public abstract class RTreeIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(RTreeIndex.class);

  protected int featureDim;
  protected int seriesLength;

  /**
   * MBRIndex supports two features: POINT and RANGE. Point features: {@code corner = float [dim]}
   *
   * <p>Range feature contains two parts: {@code corner = float [dim] range = float [dim].}
   *
   * <p>The range of dimension {@code i} is {@code [corner[i], corner[i]+range[i]]}
   */
  private final boolean usePointType;
  /** For generality, RTree only store ids of identifiers or others. */
  private RTree<PartialPath> rTree;

  protected float[] currentLowerBounds;
  protected float[] currentUpperBounds;
  //  protected double[] patterns;
  //  protected double threshold;
  //  private int amortizedPerInputCost;
  private File featureFile;
  private PartialPath currentInsertPath;

  public RTreeIndex(
      PartialPath indexSeries,
      TSDataType tsDataType,
      String indexDir,
      IndexInfo indexInfo,
      boolean usePointType) {
    super(indexSeries, tsDataType, indexInfo);
    this.usePointType = usePointType;
    initRTree();
    featureFile = IndexUtils.getIndexFile(indexDir + File.separator + "rTreeFeature");
    File indexDirFile = IndexUtils.getIndexFile(indexDir);
    if (indexDirFile.exists()) {
      logger.info("reload index {} from {}", RTREE_PAA, indexDir);
      deserializeFeatures();
    } else {
      indexDirFile.mkdirs();
    }
  }

  private void deserializeFeatures() {
    if (!featureFile.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(featureFile)) {
      this.rTree = RTree.deserializePartialPath(inputStream);
    } catch (IOException e) {
      logger.error("Error when deserialize ELB features. Given up.", e);
    }
  }

  @Override
  protected void flushIndex() {
    try (OutputStream outputStream = new FileOutputStream(featureFile)) {
      rTree.serialize(outputStream);
      rTree.clear();
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  private void initRTree() {
    this.seriesLength = Integer.parseInt(props.getOrDefault(SERIES_LENGTH, DEFAULT_SERIES_LENGTH));

    this.featureDim = Integer.parseInt(props.getOrDefault(FEATURE_DIM, DEFAULT_FEATURE_DIM));
    int nMaxPerNode = Integer.parseInt(props.getOrDefault(MAX_ENTRIES, "50"));
    int nMinPerNode = Integer.parseInt(props.getOrDefault(MIN_ENTRIES, "2"));
    if (nMaxPerNode < nMinPerNode) {
      logger.warn("param error: max_entries cannot be less than min_entries, swap them");
      nMaxPerNode = Math.max(nMinPerNode, nMaxPerNode);
      nMinPerNode = Math.min(nMinPerNode, nMaxPerNode);
    }
    if (nMaxPerNode <= 1) {
      logger.warn("param error: max_entries cannot be less than 2. set to default 50");
      nMaxPerNode = 50;
    }
    //    this.amortizedPerInputCost = calcAmortizedCost(nMaxPerNode, nMinPerNode);
    SeedsPicker seedPicker = SeedsPicker.valueOf(props.getOrDefault(SEED_PICKER, "LINEAR"));
    rTree = new RTree<PartialPath>(nMaxPerNode, nMinPerNode, featureDim, seedPicker);
    currentLowerBounds = new float[featureDim];
    currentUpperBounds = new float[featureDim];
  }

  /**
   * Given a point, r tree will increase a leaf node. Besides, we need to consider the amortized
   * number of inner nodes for each input.
   *
   * <p>We adopt cautious estimation: a full {@code a}-ary tree. Each inner node in the last layer
   * has {@code b} leaf nodes. Based on this setting, we estimate how many nodes before flushing.
   *
   * <p>For r-tree, {@code a}:=max entities, {@code b}:=min entities. When flushing, there are
   * {@code n} points. The number of inner nodes is as follows:
   *
   * <p>inner_num = (a * n / b - 1)/(a - 1) The amortized cost for one point = (1 + 1 / inner_num) *
   * leaf_cost
   *
   * <p>refer to https://en.wikipedia.org/wiki/M-ary_tree
   *
   * @param a max entities
   * @param b min entities
   * @return estimation
   */
  //  @Deprecated
  //  private int calcAmortizedCost(int a, int b) {
  //    int leafNodeCost = rTreeNodeCost();
  //    // n: how many points when flushing
  //    int n = (int) (IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize()
  //        / (leafNodeCost + 3 * Long.BYTES));
  //    double amortizedInnerNodeNum;
  //    if (n < b) {
  //      return leafNodeCost;
  //    } else {
  //      float af = (float) a;
  //      float bf = (float) b;
  //      float nf = (float) n;
  //      amortizedInnerNodeNum = ((af * nf / bf - 1.) / (af - 1.));
  //    }
  //    return (int) (leafNodeCost + leafNodeCost / amortizedInnerNodeNum);
  //  }

  /**
   * 2 float arrays + T (4 bytes, an integer or a point) + boolean + 2 pointers for the parent point
   * and the children list.
   *
   * @return estimated cost.
   */
  //  private int rTreeNodeCost() {
  //    return (2 * featureDim * Float.BYTES + 4) + 1 + 2 * Integer.BYTES;
  //  }

  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  protected abstract void fillCurrentFeature();

  public IndexFeatureExtractor startFlushTask(PartialPath partialPath, TVList tvList) {
    IndexFeatureExtractor res = super.startFlushTask(partialPath, tvList);
    currentInsertPath = partialPath;
    return res;
  }

  public void endFlushTask() {
    super.endFlushTask();
    currentInsertPath = null;
  }

  @Override
  public boolean buildNext() {
    fillCurrentFeature();
    if (usePointType) {
      rTree.insert(currentLowerBounds, currentInsertPath);
    } else {
      rTree.insert(currentLowerBounds, currentUpperBounds, currentInsertPath);
    }
    return true;
  }

  /** For index building. */
  //  protected abstract BiConsumer<Integer, OutputStream> getSerializeFunc();

  /** Following three methods are for query. */
  //  protected abstract BiConsumer<String, InputStream> getDeserializeFunc();
  //  protected abstract List<Identifier> getQueryCandidates(List<Integer> candidateIds);

  /** */
  protected abstract float[] calcQueryFeature(double[] patterns);

  public static class RTreeQueryStruct {

    /** features is represented by float array */
    float[] patternFeatures;
    //    TriFunction<float[], float[], float[], Double> calcLowerDistFunc;
    //
    //    BiFunction<double[], TVList, Double> calcExactDistFunc;
    //    Function<PartialPath, TVList> loadSeriesFunc;
    private double[] patterns;
    int topK = -1;
    double threshold = -1;
  }

  public RTreeQueryStruct initQuery(Map<String, Object> queryProps) {
    RTreeQueryStruct struct = new RTreeQueryStruct();

    //    if (calcLowerDistFunc == null) {
    //      throw new IllegalIndexParamException("calcLowerDistFunc hasn't been implemented");
    //    } else {
    //      struct.calcLowerDistFunc = calcLowerDistFunc;
    //    }
    //    if (calcExactDistFunc == null) {
    //      throw new IllegalIndexParamException("calcExactDistFunc hasn't been implemented");
    //    } else {
    //      struct.calcExactDistFunc = calcExactDistFunc;
    //    }
    //    if (loadSeriesFunc == null) {
    //      throw new IllegalIndexParamException("loadSeriesFunc hasn't been implemented");
    //    } else {
    //      struct.loadSeriesFunc = loadSeriesFunc;
    //    }

    //    struct.distance = Distance.getDistance(props.getOrDefault(DISTANCE,
    // DEFAULT_RTREE_PAA_DISTANCE));
    if (queryProps.containsKey(THRESHOLD) && queryProps.containsKey(TOP_K)) {
      throw new IllegalIndexParamException("TopK and Threshold cannot be set at the same.");
    }
    if (!queryProps.containsKey(THRESHOLD) && !queryProps.containsKey(TOP_K)) {
      throw new IllegalIndexParamException("TopK and Threshold should be set at least one");
    }
    if (queryProps.containsKey(PATTERN)) {
      struct.patterns = (double[]) queryProps.get(PATTERN);
    } else {
      throw new IllegalIndexParamException("missing parameter: " + PATTERN);
    }
    if (queryProps.containsKey(THRESHOLD)) {
      struct.threshold = (double) queryProps.get(THRESHOLD);
    }
    if (queryProps.containsKey(TOP_K)) {
      struct.topK = (int) queryProps.get(TOP_K);
    }
    struct.patternFeatures = calcQueryFeature(struct.patterns);
    return struct;
  }

  private TVList readTimeSeries(PartialPath partialPath) {
    IndexUtils.breakDown("private TVList readTimeSeries(PartialPath partialPath) {");
    return null;
    //    try {
    //      for (Filter timeFilter : filterList) {
    //        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
    //            .getQueryDataSource(indexSeries, context, timeFilter);
    //        timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
    //
    //        IBatchReader reader = new SeriesRawDataBatchReader(indexSeries,
    //            Collections.singleton(indexSeries.getMeasurement()), tsDataType, context,
    //            queryDataSource, timeFilter, null, null, true);
    //        ELBMatchFeatureExtractor featureExtractor = new ELBMatchFeatureExtractor(tsDataType,
    //            struct.pattern.length, blockWidth, elbType, true);
    ////        System.out.println(">>>>>>>>>>>>>>>> Time range: " + timeFilter);
    //        while (reader.hasNextBatch()) {
    //          BatchData batch = reader.nextBatch();
    //          featureExtractor.appendNewSrcData(batch);
    //          while (featureExtractor.hasNext()) {
    //            featureExtractor.processNext();
    //            TVListPointer p = featureExtractor.getCurrent_L2_AlignedSequence();
    ////            System.out.println(">>>>>>>>>>>>>>>> " + IndexUtils.tvListToStr(p.tvList,
    // p.offset, p.length));
    //            if (struct.elb.exactDistanceCalc(p.tvList, p.offset)) {
    //              TVList series = TVListAllocator.getInstance().allocate(tsDataType);
    //              TVList.append(series, p.tvList, p.offset, p.length);
    //              res.add(series);
    //            }
    //          }
    //          featureExtractor.clearProcessedSrcData();
    //        }
    //        reader.close();
    //      }
    //
    //    } catch (StorageEngineException | QueryProcessException | IOException e) {
    //      throw new QueryIndexException(e.getMessage());
    //    }
  }

  /**
   * to be initialized by child class.
   *
   * <p>input both sides of boundaries (double[], double[]) of a node, a pattern(double[]), return
   * the lower-boundary distance.
   *
   * <p>input: float[]: lower bounds, float[]: upper bounds, float[] pattern features
   *
   * <p>return: lower-bounding distance
   */
  protected abstract TriFunction<float[], float[], float[], Double> getCalcLowerDistFunc();

  /**
   * to be initialized by child class.
   *
   * <p>input a pattern(double[]), a loaded time series ({@link TVList}), return there real
   * distance.
   */
  protected abstract BiFunction<double[], TVList, Double> getCalcExactDistFunc();

  protected abstract IndexFeatureExtractor createQueryFeatureExtractor();

  /**
   * to be initialized by child class.
   *
   * <p>input a path({@link PartialPath}), load and return a series ({@link TVList})
   */
  protected Function<PartialPath, TVList> getLoadSeriesFunc(
      QueryContext context, IndexFeatureExtractor featureExtractor) {
    return path -> {
      QueryDataSource queryDataSource;
      TVList res = null;
      try {
        queryDataSource =
            QueryResourceManager.getInstance().getQueryDataSource(path, context, null);

        IBatchReader reader =
            new SeriesRawDataBatchReader(
                path,
                Collections.singleton(path.getMeasurement()),
                tsDataType,
                context,
                queryDataSource,
                null,
                null,
                null,
                true);
        TVList tvList = TVListAllocator.getInstance().allocate(tsDataType);
        while (reader.hasNextBatch()) {
          BatchData batch = reader.nextBatch();
          TVList.appendAll(tvList, batch);
        }
        featureExtractor.appendNewSrcData(tvList);
        reader.close();
        if (featureExtractor.hasNext()) {
          featureExtractor.processNext();
          res = (TVList) featureExtractor.getCurrent_L2_AlignedSequence();
        }
        featureExtractor.clearProcessedSrcData();
      } catch (StorageEngineException | IOException | QueryProcessException e) {
        e.printStackTrace();
      }
      return res;
    };
  }

  /**
   * Search with PMR-Quadtree [1], which is for any hierarchical index structure that is constructed
   * using a conservative and recursive partitioning of the data [2].
   *
   * <p>[1] G. R. Hjaltason and H. Samet. Ranking in Spatial Databases. In Proceedings of the 4th
   * International Symposium on Advances in Spatial Databases, SSD ’95, pages 83–95, Berlin,
   * Heidelberg, 1995. Springer- Verlag.
   *
   * <p>[2] S. Berchtold, C. B ̈ohm, D. A. Keim, and H.-P. Kriegel. A Cost Model for Nearest
   * Neighbor Search in High- dimensional Data Space. In Proceedings of the Six- teenth ACM
   * SIGACT-SIGMOD-SIGART Symposium on Principles of Database Systems, PODS ’97, pages 78–86, New
   * York, NY, USA, 1997. ACM.
   */
  public QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable iIndexUsable,
      QueryContext context,
      IIndexCandidateOrderOptimize candidateOrderOptimize,
      boolean alignedByTime)
      throws QueryIndexException {
    RTreeQueryStruct struct = initQuery(queryProps);
    List<DistSeries> res;
    res =
        rTree.exactTopKSearch(
            struct.topK,
            struct.patterns,
            struct.patternFeatures,
            ((MultiShortIndexUsability) iIndexUsable).getUnusableRange(),
            getCalcLowerDistFunc(),
            getCalcExactDistFunc(),
            getLoadSeriesFunc(context, createQueryFeatureExtractor()));
    for (DistSeries ds : res) {
      ds.partialPath = ds.partialPath.concatNode(String.format("(D=%.2f)", ds.dist));
    }
    return constructSearchDataset(res, alignedByTime);
  }

  //  public int postProcessNext(List<IndexFuncResult> funcResult) throws QueryIndexException {
  //    Identifier identifier = indexFeatureExtractor.getCurrent_L1_Identifier();
  //    TVList srcList = indexFeatureExtractor
  //        .get_L0_SourceData(identifier.getStartTime(), identifier.getEndTime());
  //    TVList aligned = IndexUtils.alignUniform(srcList, patterns.length);
  //    double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
  //    System.out.println(String.format(
  //        "PAA Process: ed:%.3f: %s", ed, IndexUtils.tvListToStr(aligned)));
  //    int reminding = funcResult.size();
  //    if (ed <= threshold) {
  //      for (IndexFuncResult result : funcResult) {
  //        if (result.getIndexFunc() == IndexFunc.ED) {
  //          result.addScalar(ed);
  //        } else {
  //          IndexFuncFactory.basicSimilarityCalc(result, indexFeatureExtractor, patterns);
  //        }
  //      }
  //    }
  //    TVListAllocator.getInstance().release(aligned);
  //
  //    return reminding;
  //  }

  protected abstract double calcLowerBoundThreshold(double queryThreshold);

  @Override
  public String toString() {
    return rTree.toString();
  }
}
