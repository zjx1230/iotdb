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

import static org.apache.iotdb.db.index.common.IndexConstant.FEATURE_DIM;
import static org.apache.iotdb.db.index.common.IndexConstant.MAX_ENTRIES;
import static org.apache.iotdb.db.index.common.IndexConstant.MIN_ENTRIES;
import static org.apache.iotdb.db.index.common.IndexConstant.SEED_PICKER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.algorithm.RTree.SeedsPicker;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>MBRIndex extracts features for input series data and put them into a root leaf node
 * initially. When the size of a leaf node achieve the specified threshold, the leaf node will
 * split. MBRIndex constructs hierarchical inner nodes above leaf nodes according to a lower
 * bounding constraint for guaranteeing no-false-dismissals pruning.</p>
 *
 * For all indexes that applies MBR rules, serialization and deserialization methods for features
 * are required.
 */

public abstract class MBRIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(MBRIndex.class);

  protected int featureDim;
  /**
   * <p>MBRIndex supports two features: POINT and RANGE.</p>
   * Point features: {@code corner = float [dim]}
   * <p>Range feature contains two parts:</p>
   * {@code corner = float [dim] range = float [dim].}
   * <p>The range of dimension {@code i} is {@code [corner[i], corner[i]+range[i]]}</p>
   */
  private final boolean usePointType;
  /**
   * For generality, RTree only store ids of identifiers or others.
   */
  private RTree<Integer> rTree;
  protected float[] currentLowerBounds;
  protected float[] currentUpperBounds;
  protected double[] patterns;
  protected double threshold;
  private int amortizedPerInputCost;

  public MBRIndex(String path, IndexInfo indexInfo, boolean usePointType) {
    super(path, indexInfo);
    this.usePointType = usePointType;
    initRTree();
  }

  private void initRTree() {
    this.featureDim = Integer.parseInt(props.getOrDefault(FEATURE_DIM, "4"));
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
    this.amortizedPerInputCost = calcAmortizedCost(nMaxPerNode, nMinPerNode);
    SeedsPicker seedPicker = SeedsPicker.valueOf(props.getOrDefault(SEED_PICKER, "LINEAR"));
    rTree = new RTree<>(nMaxPerNode, nMinPerNode, featureDim, seedPicker);
    currentLowerBounds = new float[featureDim];
    currentUpperBounds = new float[featureDim];

  }

  /**
   * Given a point, r tree will increase a leaf node. Besides, we need to consider the amortized
   * number of inner nodes for each input.
   *
   * We adopt  cautious estimation: a full {@code a}-ary tree. Each inner node in the last layer has
   * {@code b} leaf nodes. Based on this setting, we estimate how many nodes before flushing.
   *
   * For r-tree, {@code a}:=max entities, {@code b}:=min entities. When flushing, there are {@code
   * n} points. The number of inner nodes is as follows:
   *
   * <p>inner_num = (a * n / b - 1)/(a - 1)</p>
   *
   * The amortized cost for one point = (1 + 1 / inner_num) * leaf_cost
   *
   *
   * refer to https://en.wikipedia.org/wiki/M-ary_tree
   *
   * @param a max entities
   * @param b min entities
   * @return estimation
   */
  private int calcAmortizedCost(int a, int b) {
    int leafNodeCost = rTreeNodeCost();
    // n: how many points when flushing
    int n = (int) (IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize()
        / (leafNodeCost + 3 * Long.BYTES));
    double amortizedInnerNodeNum;
    if (n < b) {
      return leafNodeCost;
    } else {
      float af = (float) a;
      float bf = (float) b;
      float nf = (float) n;
      amortizedInnerNodeNum = ((af * nf / bf - 1.) / (af - 1.));
    }
    return (int) (leafNodeCost + leafNodeCost / amortizedInnerNodeNum);
  }


  /**
   * 2 float arrays + T (4 bytes, an integer or a point) + boolean + 2 pointers for the parent point
   * and the children list.
   *
   * @return estimated cost.
   */
  private int rTreeNodeCost() {
    return (2 * featureDim * Float.BYTES + 4) + 1 + 2 * Integer.BYTES;
  }

  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  protected abstract int fillCurrentFeature();

  @Override
  public boolean buildNext() {
    int currentIdx = fillCurrentFeature();
    if (usePointType) {
      rTree.insert(currentLowerBounds, currentIdx);
    } else {
      rTree.insert(currentLowerBounds, currentUpperBounds, currentIdx);
    }
    return true;
  }

  /**
   * For index building.
   */
  protected abstract BiConsumer<Integer, OutputStream> getSerializeFunc();

  /**
   * Following three methods are for query.
   */
  protected abstract BiConsumer<Integer, ByteBuffer> getDeserializeFunc();

  protected abstract List<Identifier> getQueryCandidates(List<Integer> candidateIds);

  /**
   *
   */
  protected abstract void calcAndFillQueryFeature();

  @Override
  public IndexFlushChunk flush() {
    if (indexPreprocessor.getCurrentChunkSize() == 0) {
      logger.warn("Nothing to be flushed, directly return null");
      System.out.println("Nothing to be flushed, directly return null");
      return null;
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // serialize RTree
    BiConsumer<Integer, OutputStream> biConsumer = getSerializeFunc();
    try {
      rTree.serialize(outputStream, biConsumer);
    } catch (IOException e) {
      logger.error("flush failed", e);
      return null;
    }
    long st = indexPreprocessor.getChunkStartTime();
    long end = indexPreprocessor.getChunkEndTime();
    return new IndexFlushChunk(path, indexType, outputStream, st, end);
  }

  /**
   *
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public long clear() {
    int estimateSize = indexPreprocessor.getCurrentChunkSize() * amortizedPerInputCost;
    estimateSize += super.clear();
    initRTree();
    return estimateSize;
  }

  /**
   * All it needs depends on its preprocessor. Just for explain.
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public int getAmortizedSize() {
    return super.getAmortizedSize() + this.amortizedPerInputCost;
  }


  @Override
  public List<Identifier> queryByIndex(ByteBuffer indexChunkData) throws IndexManagerException {
    calcAndFillQueryFeature();
    RTree<Integer> chunkRTree = RTree.deserialize(indexChunkData, getDeserializeFunc());
    double lowerBoundThreshold = calcLowerBoundThreshold(threshold);
    List<Integer> candidateIds = chunkRTree
        .searchWithThreshold(currentLowerBounds, currentUpperBounds, lowerBoundThreshold);
    return getQueryCandidates(candidateIds);
  }

  protected abstract double calcLowerBoundThreshold(double queryThreshold);

}
