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
package org.apache.iotdb.db.index.algorithm.rtree;

import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.index.common.DistSeries;
import org.apache.iotdb.db.index.common.TriFunction;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A simple implementation of R-Tree, referring to the article: Guttman, Antonin. "R-trees: A
 * dynamic index structure for spatial searching." SIGMOD 1984
 */
public class RTree<T> {

  private static final Logger logger = LoggerFactory.getLogger(RTree.class);

  private static final int INNER_NODE = 0;
  private static final int LEAF_NODE = 1;
  private static final int ITEM = 2;
  private static final float VAGUE_ERROR = 0.0001f;

  final int dim;
  final int nMaxPerNode;
  final int nMinPerNode;
  private final SeedsPicker seedsPicker;
  private final Random r;
  RNode root;

  public RTree(int nMaxPerNode, int nMinPerNode, int dim) {
    this(nMaxPerNode, nMinPerNode, dim, SeedsPicker.LINEAR);
  }

  public RTree(int nMaxPerNode, int nMinPerNode, int dim, SeedsPicker seedsPicker) {
    if (nMaxPerNode < 2 * nMinPerNode) {
      throw new IndexRuntimeException(
          "For RTree, nMaxPerNode should be larger than 2 * nMinPerNode");
    }
    this.dim = dim;
    this.nMaxPerNode = nMaxPerNode;
    this.nMinPerNode = nMinPerNode;
    this.seedsPicker = seedsPicker;
    root = initRoot(true);
    this.r = new Random(0);
  }

  private RNode initRoot(boolean isLeaf) {
    float[] lbs = new float[dim];
    float[] ubs = new float[dim];
    for (int i = 0; i < dim; i++) {
      lbs[i] = Float.MAX_VALUE;
      ubs[i] = -Float.MAX_VALUE;
    }
    return new RNode(lbs, ubs, isLeaf);
  }

  private RNode chooseLeaf(RNode node, Item item) {
    if (node.isLeaf) {
      return node;
    }
    float minAreaInc = Float.MAX_VALUE;
    RNode bsf = null;
    float bsfArea = -1;
    for (RNode child : node.children) {
      float childInc = calcExpandingArea(child, item);
      if (childInc < minAreaInc) {
        minAreaInc = childInc;
        bsf = child;
        bsfArea = -1;
      } else if (childInc == minAreaInc) {
        if (bsfArea == -1) {
          bsfArea = calcArea(bsf);
        }
        float childArea = calcArea(child);
        if (childArea < bsfArea) {
          bsf = child;
          bsfArea = childArea;
        }
      }
    }
    return chooseLeaf(bsf, item);
  }

  private void tightenBound(RNode node) {
    if (node.children.isEmpty()) {
      return;
    }
    if (node.children.size() == 1) {
      RNode onlyChild = node.children.get(0);
      System.arraycopy(onlyChild.lbs, 0, node.lbs, 0, dim);
      System.arraycopy(onlyChild.ubs, 0, node.ubs, 0, dim);
      return;
    }
    for (int i = 0; i < dim; i++) {
      float minLb = Float.MAX_VALUE;
      float maxUb = -Float.MAX_VALUE;
      for (RNode child : node.children) {
        if (child.lbs[i] < minLb) {
          minLb = child.lbs[i];
        }
        if (child.ubs[i] > maxUb) {
          maxUb = child.ubs[i];
        }
      }
      node.lbs[i] = minLb;
      node.ubs[i] = maxUb;
      assert node.lbs[i] <= node.ubs[i]
          && node.lbs[i] < Float.MAX_VALUE
          && node.ubs[i] > -Float.MAX_VALUE;
    }
  }

  /**
   * If the size of node children > nMaxPerNode, process bottom-up split from node to root.
   *
   * @param node might need to be split.
   */
  private void splitNode(RNode node) {
    if (node.getChildren().size() <= nMaxPerNode) {
      return;
    }
    RNode[] twoNodes = splitIntoTwoNodes(node);
    if (node == root) {
      root = initRoot(false);
      addNodeToChildrenList(root, twoNodes[0]);
      addNodeToChildrenList(root, twoNodes[1]);
    } else {
      splitNode(node.parent);
    }
  }

  private static void addNodeToChildrenList(RNode parent, RNode child) {
    parent.children.add(child);
    child.parent = parent;
  }

  private static void addNodesToChildrenList(RNode parent, List<RNode> children) {
    parent.children.addAll(children);
    children.forEach(child -> child.parent = parent);
  }

  /**
   * Given node has too many children, we move a part of them into a new Node, and add the new one
   * to its parent node's children list.
   *
   * @param node the number of its children must be larger than the nMax.
   */
  private RNode[] splitIntoTwoNodes(RNode node) {
    assert node.children.size() > nMaxPerNode;
    RNode newNode = new RNode(node.lbs, node.ubs, node.isLeaf);
    if (node != root) {
      addNodeToChildrenList(node.parent, newNode);
    }
    LinkedList<RNode> candidates = new LinkedList<>(node.children);
    int nMaxChildren = candidates.size() - nMinPerNode;
    node.children.clear();

    RNode[] seeds = seedsPicker.pickSeeds(candidates);
    addNodeToChildrenList(node, seeds[0]);
    addNodeToChildrenList(newNode, seeds[1]);
    tightenBound(node);
    tightenBound(newNode);
    while (!candidates.isEmpty()) {
      // guarantee that both of two nodes have at least nMinPerNode children.
      RNode lessNode = null;
      if (node.children.size() == nMaxChildren) {
        lessNode = newNode;
      }
      if (newNode.children.size() == nMaxChildren) {
        lessNode = node;
      }
      if (lessNode != null) {
        addNodesToChildrenList(lessNode, candidates);
        candidates.clear();
        tightenBound(lessNode); // Not sure this is required.
        return new RNode[] {node, newNode};
      }
      // classical R-Tree insert rule.
      RNode nextNode = seedsPicker.next(candidates);
      RNode chosen;
      float e0 = calcExpandingArea(node, nextNode);
      float e1 = calcExpandingArea(newNode, nextNode);
      if (e0 < e1) {
        chosen = node;
      } else if (e0 > e1) {
        chosen = newNode;
      } else {
        float a0 = calcArea(node);
        float a1 = calcArea(newNode);
        if (a0 < a1) {
          chosen = node;
        } else if (e0 > a1) {
          chosen = newNode;
        } else {
          if (node.children.size() < newNode.children.size()) {
            chosen = node;
          } else if (node.children.size() > newNode.children.size()) {
            chosen = newNode;
          } else {
            chosen = r.nextBoolean() ? node : newNode;
          }
        }
      }
      addNodeToChildrenList(chosen, nextNode);
      tightenBound(chosen);
    }
    return new RNode[] {node, newNode};
  }

  /** Clear the RTree */
  public void clear() {
    root = initRoot(true);
  }

  /**
   * Insert a high-dim point into RTree.
   *
   * @param cs point coordinates
   * @param v value
   */
  public void insert(float[] cs, final T v) {
    insert(cs, cs, v);
  }

  /** Insert a rectangle into RTree. */
  @SuppressWarnings("unchecked")
  public void insert(float[] lbs, float[] ubs, final T v) {
    if (lbs.length != dim || ubs.length != dim) {
      throw new IndexRuntimeException("The dimension of the insert point doesn't match");
    }
    Item item = new Item(ubs, lbs, v);
    RNode leaf = chooseLeaf(root, item);
    addNodeToChildrenList(leaf, item);
    if (leaf.children.size() > nMaxPerNode) {
      splitNode(leaf);
    }
    // tight leaf and all its ancestors.
    tightenBound(leaf);
    while (leaf.parent != null) {
      tightenBound(leaf.parent);
      leaf = leaf.parent;
    }
  }

  /**
   * Given a point(or rectangle), find all intersected items in the tree.
   *
   * @param lbs a dim-dimension array, the lower bounds of rectangle.
   * @param ubs a dim-dimension array, the upper bounds of rectangle. For point, lbs == ubs.
   * @return a list of intersected items
   */
  public List<T> search(float[] lbs, float[] ubs) {
    assert (lbs.length == dim);
    assert (ubs.length == dim);
    LinkedList<T> res = new LinkedList<>();
    RNode query = new RNode(lbs, ubs, true);
    search(query, root, res);
    return res;
  }

  @SuppressWarnings("unchecked")
  private void search(RNode query, RNode node, LinkedList<T> res) {
    if (node.isLeaf) {
      for (RNode child : node.children) {
        if (isIntersect(query, child)) {
          res.add(((Item<T>) child).v);
        }
      }
    } else {
      for (RNode child : node.children) {
        if (isIntersect(query, child)) {
          search(query, child, res);
        }
      }
    }
  }

  public List<T> searchWithThreshold(float[] lbs, float[] ubs, double threshold) {
    assert (lbs.length == dim);
    assert (ubs.length == dim);
    LinkedList<T> res = new LinkedList<>();
    RNode query = new RNode(lbs, ubs, true);
    searchWithThreshold(query, root, res, threshold);
    return res;
  }

  @SuppressWarnings("unchecked")
  private void searchWithThreshold(RNode query, RNode node, LinkedList<T> res, double threshold) {
    if (node.isLeaf) {
      for (RNode child : node.children) {
        if (mbrED(query, child) <= threshold + VAGUE_ERROR) {
          res.add(((Item<T>) child).v);
        }
      }
    } else {
      for (RNode child : node.children) {
        if (mbrED(query, child) <= threshold + VAGUE_ERROR) {
          searchWithThreshold(query, child, res, threshold);
        }
      }
    }
  }

  private double mbrED(RNode a, RNode b) {
    float dist = 0;
    for (int i = 0; i < a.children.size(); i++) {
      float al = a.lbs[i];
      float au = a.ubs[i];
      float bl = b.lbs[i];
      float bu = b.ubs[i];
      if (al > bu) {
        dist += (al - bu) * (al - bu);
      } else if (bl > au) {
        dist += (bl - au) * (bl - au);
      }
    }
    return Math.sqrt(dist);
  }

  /** Returns the increase in area necessary for the given rectangle to cover the given entry. */
  private float calcExpandingArea(RNode oriNode, RNode newNode) {
    float oriArea = calcArea(oriNode);
    float expandArea = 1.0f;
    for (int i = 0; i < dim; i++) {
      float expandLb = Math.min(newNode.lbs[i], oriNode.lbs[i]);
      float expandUb = Math.max(newNode.ubs[i], oriNode.ubs[i]);
      expandArea *= expandUb - expandLb;
    }
    return (expandArea - oriArea);
  }

  private float calcArea(RNode node) {
    float area = 1.0f;
    for (int i = 0; i < dim; i++) {
      area *= node.ubs[i] - node.lbs[i];
    }
    return area;
  }

  private boolean isIntersect(RNode n1, RNode n2) {
    for (int i = 0; i < dim; i++) {
      if ((n2.lbs[i] - n1.ubs[i] > VAGUE_ERROR) || (n1.lbs[i] - n2.ubs[i] > VAGUE_ERROR)) {
        return false;
      }
    }
    return true;
  }

  /**
   * write metadata
   *
   * @param outputStream serialize to
   */
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(dim, outputStream);
    ReadWriteIOUtils.write(nMaxPerNode, outputStream);
    ReadWriteIOUtils.write(nMinPerNode, outputStream);
    ReadWriteIOUtils.write(seedsPicker.serialize(), outputStream);
    serialize(root, outputStream);
  }

  /**
   * Pre-order traversal
   *
   * @param outputStream serialize to
   */
  @SuppressWarnings("unchecked")
  private void serialize(RNode node, OutputStream outputStream) throws IOException {
    if (node instanceof Item) {
      ReadWriteIOUtils.write(ITEM, outputStream);
    } else if (node.isLeaf) {
      ReadWriteIOUtils.write(LEAF_NODE, outputStream);
    } else {
      ReadWriteIOUtils.write(INNER_NODE, outputStream);
    }
    for (float lb : node.lbs) {
      ReadWriteIOUtils.write(lb, outputStream);
    }
    for (float ub : node.ubs) {
      ReadWriteIOUtils.write(ub, outputStream);
    }
    if (node instanceof Item) {
      T value = ((Item<T>) node).v;
      ReadWriteIOUtils.write(value.toString(), outputStream);
      //      biConsumer.accept(value, outputStream);
    } else {
      // write child
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      for (RNode child : node.getChildren()) {
        serialize(child, outputStream);
      }
    }
  }

  public static RTree<PartialPath> deserializePartialPath(InputStream inputStream)
      throws IOException {
    int dim = ReadWriteIOUtils.readInt(inputStream);
    int nMaxPerNode = ReadWriteIOUtils.readInt(inputStream);
    int nMinPerNode = ReadWriteIOUtils.readInt(inputStream);
    SeedsPicker seedsPicker = SeedsPicker.deserialize(ReadWriteIOUtils.readShort(inputStream));
    RTree<PartialPath> rTree = new RTree<>(nMaxPerNode, nMinPerNode, dim, seedsPicker);
    rTree.deserialize(
        rTree,
        null,
        inputStream,
        in -> {
          try {
            return new PartialPath(ReadWriteIOUtils.readString(inputStream));
          } catch (IOException | IllegalPathException e) {
            logger.error("read path error", e);
            return null;
          }
        });
    return rTree;
  }

  /**
   * Pre-order traversal
   *
   * @param inputStream deserialize from
   */
  private void deserialize(
      RTree rTree,
      RNode parent,
      InputStream inputStream,
      Function<InputStream, T> deserializeItemFunc)
      throws IOException {
    int nodeType = ReadWriteIOUtils.readInt(inputStream);
    float[] lbs = new float[rTree.dim];
    float[] ubs = new float[rTree.dim];
    for (int i = 0; i < rTree.dim; i++) {
      lbs[i] = ReadWriteIOUtils.readFloat(inputStream);
    }
    for (int i = 0; i < rTree.dim; i++) {
      ubs[i] = ReadWriteIOUtils.readFloat(inputStream);
    }
    RNode node;
    if (nodeType == ITEM) {
      //      T value = ReadWriteIOUtils.readString(inputStream);
      T value = deserializeItemFunc.apply(inputStream);
      node = new Item<>(lbs, ubs, value);
    } else {
      node = new RNode(lbs, ubs, nodeType == LEAF_NODE);
      int childSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < childSize; i++) {
        deserialize(rTree, node, inputStream, deserializeItemFunc);
      }
    }
    if (parent == null) {
      rTree.root = node;
    } else {
      addNodeToChildrenList(parent, node);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("nMax:%d,", nMaxPerNode));
    sb.append(String.format("nMin:%d,", nMinPerNode));
    sb.append(String.format("dim:%d,", dim));
    sb.append(String.format("seedsPicker:%s%n", seedsPicker));
    if (root == null) {
      return sb.toString();
    }
    toString(root, 0, sb);
    return sb.toString();
  }

  public String toDetailedString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("nMax:%d,", nMaxPerNode));
    sb.append(String.format("nMin:%d,", nMinPerNode));
    sb.append(String.format("dim:%d,", dim));
    sb.append(String.format("seedsPicker:%s%n", seedsPicker));
    if (root == null) {
      return sb.toString();
    }
    toString(root, 0, sb);
    return sb.toString();
  }

  private void toString(RNode node, int depth, StringBuilder sb) {
    for (int i = 0; i < depth; i++) {
      sb.append("--");
    }
    sb.append(node.toString());
    sb.append("\n");
    if (!(node instanceof Item)) {
      for (RNode child : node.children) {
        toString(child, depth + 1, sb);
      }
    }
  }

  static class RNode {

    final float[] lbs;
    final float[] ubs;
    boolean isLeaf;
    RNode parent;

    LinkedList<RNode> children;

    private RNode(float[] lbs, float[] ubs, boolean isLeaf) {
      this.lbs = new float[lbs.length];
      this.ubs = new float[ubs.length];
      System.arraycopy(lbs, 0, this.lbs, 0, lbs.length);
      System.arraycopy(ubs, 0, this.ubs, 0, ubs.length);
      children = new LinkedList<>();
      this.isLeaf = isLeaf;
    }

    public int size() {
      return children.size();
    }

    public List<RNode> getChildren() {
      return children;
    }

    @Override
    public String toString() {
      return "RNode{"
          + "LB="
          + Arrays.toString(lbs)
          + ", UB="
          + Arrays.toString(ubs)
          + ", leaf="
          + isLeaf
          + '}';
    }
  }

  private static class Item<T> extends RNode {

    private final T v;

    private Item(float[] lbs, float[] ubs, T v) {
      super(lbs, ubs, true);
      this.v = v;
    }

    //    public T getValue() {
    //      return v;
    //    }

    @Override
    public String toString() {
      return "Item: " + v + "," + super.toString();
    }
  }

  public enum SeedsPicker {
    LINEAR {
      @Override
      public RNode[] pickSeeds(LinkedList<RNode> candidates) {
        int dim = candidates.get(0).lbs.length;
        RNode[] bestSeeds = new RNode[2];
        double bestScore = -1.0d;
        for (int i = 0; i < dim; i++) {
          float minLb = Float.MAX_VALUE;
          float minUb = Float.MAX_VALUE;
          float maxUb = -Float.MAX_VALUE;
          float maxLb = -Float.MAX_VALUE;
          RNode maxLbNode = null;
          RNode minUbNode = null;
          for (RNode node : candidates) {
            if (node.lbs[i] < minLb) {
              minLb = node.lbs[i];
            }
            if (node.lbs[i] > maxLb) {
              maxLb = node.lbs[i];
              maxLbNode = node;
            }
            if (node.ubs[i] > maxUb) {
              maxUb = node.ubs[i];
            }
            if (node.ubs[i] < minUb) {
              minUb = node.ubs[i];
              minUbNode = node;
            }
          }
          double splitScore = Math.abs((minUb - maxLb) / (maxUb - minLb + 0.001));
          if (minUbNode != maxLbNode && splitScore > bestScore) {
            bestScore = splitScore;
            bestSeeds[0] = maxLbNode;
            bestSeeds[1] = minUbNode;
          }
        }
        // No better split found.
        if (bestScore == -1.0d) {
          bestSeeds[0] = candidates.get(0);
          bestSeeds[1] = candidates.get(1);
        }
        candidates.remove(bestSeeds[0]);
        candidates.remove(bestSeeds[1]);
        return bestSeeds;
      }

      @Override
      public RNode next(LinkedList<RNode> candidates) {
        return candidates.pop();
      }
    };

    /**
     * judge the seeds picker type.
     *
     * @param i an integer used to determine index type
     * @return index type
     */
    public static SeedsPicker deserialize(short i) {
      switch (i) {
        case 0:
          return LINEAR;
        default:
          throw new IllegalIndexParamException("Given seeds picker is not implemented");
      }
    }

    public static int getSerializedSize() {
      return Short.BYTES;
    }

    /** @return the integer used to determine index type */
    public short serialize() {
      switch (this) {
        case LINEAR:
          return 0;
        default:
          throw new IllegalIndexParamException("Given seeds picker is not implemented");
      }
    }

    public abstract RNode[] pickSeeds(LinkedList<RNode> candidates);

    public abstract RNode next(LinkedList<RNode> candidates);
  }

  /** A disturbing function with so many interface functions! */
  public List<DistSeries> exactTopKSearch(
      int topK,
      double[] queryTs,
      float[] patternFeatures,
      Set<PartialPath> modifiedPaths,
      TriFunction<float[], float[], float[], Double> calcLowerBoundDistFunc,
      BiFunction<double[], TVList, Double> calcRealDistFunc,
      Function<PartialPath, TVList> loadSeriesFunc) {
    RNode approxNode = approximateSearch(patternFeatures);
    Pair<List<PartialPath>, List<TVList>> pairs =
        readSeriesFromBinaryFileAtOnceWithIdx(
            approxNode,
            loadSeriesFunc,
            modifiedPaths,
            Double.MAX_VALUE,
            calcLowerBoundDistFunc,
            patternFeatures);
    PqItem bsfAnswer = new PqItem();
    PriorityQueue<DistSeries> topKPQ = new PriorityQueue<>(topK, new DistSeriesComparator());
    bsfAnswer.dist =
        minTopKDistOfNode(pairs.right, pairs.left, queryTs, topKPQ, calcRealDistFunc, topK);

    // initialize priority queue
    Comparator<PqItem> comparator = new DistComparator();
    PriorityQueue<PqItem> pq = new PriorityQueue<>(comparator);

    // initialize the priority queue
    PqItem tempItem = new PqItem();
    tempItem.node = root;
    tempItem.dist = calcLowerBoundDistFunc.apply(root.lbs, root.ubs, patternFeatures);
    pq.add(tempItem);
    //    int lastMislstone = 100000;
    // process the priority queue
    //    int leafCount = 1;
    //    int calcDistCount = bsfAnswer.node.size();
    //    int processTerminalCount = 1;
    //    int processInnerCount = 0;
    PqItem minPqItem;
    while (!pq.isEmpty()) {
      minPqItem = pq.remove();
      if (minPqItem.dist > bsfAnswer.dist) {
        break;
      }
      if (minPqItem.node.isLeaf) {
        if (minPqItem.node == approxNode) {
          continue;
        }

        //        leafCount++;
        // verify the true distance,replace the estimate with the true dist
        //        calcDistCount += minPqItem.node.size();
        //        processTerminalCount++;

        pairs =
            readSeriesFromBinaryFileAtOnceWithIdx(
                minPqItem.node,
                loadSeriesFunc,
                modifiedPaths,
                bsfAnswer.dist,
                calcLowerBoundDistFunc,
                patternFeatures);
        bsfAnswer.dist =
            minTopKDistOfNode(pairs.right, pairs.left, queryTs, topKPQ, calcRealDistFunc, topK);

      } else {
        //        processInnerCount++;
        // minPqItem is internal
        // for left
        for (RNode child : minPqItem.node.children) {
          tempItem = new PqItem();
          tempItem.node = child;
          tempItem.dist = calcLowerBoundDistFunc.apply(child.lbs, child.ubs, patternFeatures);

          if (tempItem.dist < bsfAnswer.dist) {
            pq.add(tempItem);
          }
        }
      }
    }
    // merge modified parts
    List<TVList> modifiedSeries = readModifiedSeries(modifiedPaths, loadSeriesFunc);
    bsfAnswer.dist =
        minTopKDistOfNode(
            modifiedSeries,
            new ArrayList<>(modifiedPaths),
            queryTs,
            topKPQ,
            calcRealDistFunc,
            topK);

    if (topKPQ.isEmpty()) {
      return Collections.emptyList();
    } else {
      int retSize = Math.min(topK, topKPQ.size());
      DistSeries[] res = new DistSeries[retSize];
      int idx = retSize - 1;
      while (!topKPQ.isEmpty()) {
        DistSeries distSeries = topKPQ.poll();
        res[idx--] = distSeries;
      }
      return Arrays.asList(res);
    }
  }

  private RNode approximateSearch(float[] patterns) {
    Item item = new Item<>(patterns, patterns, null);
    return chooseLeaf(root, item);
  }

  private double minTopKDistOfNode(
      List<TVList> tss,
      List<PartialPath> paths,
      double[] queryTs,
      PriorityQueue<DistSeries> topKPQ,
      BiFunction<double[], TVList, Double> calcRealDistFunc,
      final int K) {
    double kthMinDist =
        (topKPQ.size() < K || topKPQ.peek() == null) ? Double.MAX_VALUE : topKPQ.peek().dist;
    double tempDist;
    //    double sumExactDist = 0;
    //    double minExactDist = Double.MAX_VALUE;
    int acceptCount = 0;
    for (int i = 0; i < tss.size(); i++) {
      TVList ts = tss.get(i);
      PartialPath partialPath = paths.get(i);

      tempDist = calcRealDistFunc.apply(queryTs, ts);
      //      sumExactDist += tempDist;
      //      if (tempDist < minExactDist) {
      //        minExactDist = tempDist;
      //      }
      if (topKPQ.size() < K || tempDist < kthMinDist) {
        if (topKPQ.size() == K) {
          topKPQ.poll();
        }
        topKPQ.add(new DistSeries(tempDist, ts, partialPath));
        kthMinDist = topKPQ.peek().dist;
        acceptCount++;
      }
    }
    return kthMinDist;
  }

  private List<TVList> readModifiedSeries(
      Set<PartialPath> partialPaths, Function<PartialPath, TVList> loadSeriesFunc) {
    List<TVList> tvs = new ArrayList<>();
    for (PartialPath partialPath : partialPaths) {
      tvs.add(loadSeriesFunc.apply(partialPath));
    }
    return tvs;
  }

  private Pair<List<PartialPath>, List<TVList>> readSeriesFromBinaryFileAtOnceWithIdx(
      RNode node,
      Function<PartialPath, TVList> loadSeriesFunc,
      Set<PartialPath> modifiedPaths,
      double bsfDist,
      TriFunction<float[], float[], float[], Double> calcLowerBoundDistFunc,
      float[] patternFeatures) {
    List<PartialPath> ps = new ArrayList<>();
    List<TVList> tvs = new ArrayList<>();
    for (RNode rNode : node.children) {
      PartialPath p = (PartialPath) ((Item) rNode).v;
      if (!modifiedPaths.contains(p)) {
        double tempDist = calcLowerBoundDistFunc.apply(node.lbs, node.ubs, patternFeatures);
        if (tempDist < bsfDist) {
          ps.add(p);
          tvs.add(loadSeriesFunc.apply(p));
        }
      }
    }
    return new Pair<>(ps, tvs);
  }

  private static class PqItem {

    RNode node;
    double dist;
  }

  public static class DistSeriesComparator implements Comparator<DistSeries> {

    public int compare(DistSeries item_1, DistSeries item_2) {
      return Double.compare(item_2.dist, item_1.dist);
    }
  }

  public static class DistComparator implements Comparator<PqItem> {

    public int compare(PqItem item_1, PqItem item_2) {
      return Double.compare(item_1.dist, item_2.dist);
    }
  }
}
