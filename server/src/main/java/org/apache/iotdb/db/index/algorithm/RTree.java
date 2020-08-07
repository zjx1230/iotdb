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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.common.IllegalIndexParamException;
import org.apache.iotdb.db.index.common.IndexRuntimeException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * <p>This code is an implementation of an arbitrary-dimension RTree. Refer to: R-Trees: A Dynamic
 * Index Structure for * Spatial Searching (Antonn Guttmann, 1984) This class is not
 * thread-safe.</p>
 *
 * <p>The code refer to: <a href="http://www.spf4j.org/jacoco/org.spf4j.base/RTree.java.html"></a>.
 * The license of original code is as follows:</p>
 * <p>Copyright 2010 Russ Weeks rweeks@newbrightidea.com</p>
 * <p>Under the GNU * LGPL License details here: http://www.gnu.org/licenses/lgpl-3.0.txt</p>
 */
public class RTree<T> {

  public enum SeedPicker {

    LINEAR, QUADRATIC;

    /**
     * judge the index type.
     *
     * @param i an integer used to determine index type
     * @return index type
     */
    public static SeedPicker deserialize(short i) {
      switch (i) {
        case 0:
          return LINEAR;
        case 1:
          return QUADRATIC;
        default:
          throw new IllegalIndexParamException("Given index is not implemented");
      }
    }

    public static int getSerializedSize() {
      return Short.BYTES;
    }

    /**
     * @return the integer used to determine index type
     */
    public short serialize() {
      switch (this) {
        case LINEAR:
          return 0;
        case QUADRATIC:
          return 1;
        default:
          throw new IllegalIndexParamException("Given index is not implemented");
      }
    }
  }

  private final int maxEntries;
  final int minEntries;
  private final int numDims;
  private final float[] pointDims;
  private final SeedPicker seedPicker;
  protected Node root;
  protected int size;

  private static final float DIM_FACTOR = -2.0f;
  private static final float FUDGE_FACTOR = 1.001f;

  /**
   * Creates a new RTree.
   *
   * @param maxEntries maximum number of entries per node
   * @param minEntries minimum number of entries per node (except for the root node)
   * @param numDims the number of dimensions of the RTree.
   */
  public RTree(final int maxEntries, final int minEntries, final int numDims,
      final SeedPicker seedPicker) {
    assert (minEntries <= (maxEntries / 2));
    this.numDims = numDims;
    this.maxEntries = maxEntries;
    this.minEntries = minEntries;
    this.seedPicker = seedPicker;
    pointDims = new float[numDims];
    root = buildRoot(true);
  }

  public RTree(final int maxEntries, final int minEntries, final int numDims) {
    this(maxEntries, minEntries, numDims, SeedPicker.LINEAR);
  }


  /**
   * Builds a new RTree using default parameters: maximum 50 entries per node minimum 2 entries per
   * node 2 dimensions
   */
  public RTree() {
    this(50, 2, 2, SeedPicker.LINEAR);
  }

  private Node buildRoot(final boolean asLeaf) {
    float[] initCoords = new float[numDims];
    float[] initDimensions = new float[numDims];
    for (int i = 0; i < this.numDims; i++) {
      initCoords[i] = (float) Math.sqrt(Float.MAX_VALUE);
      initDimensions[i] = DIM_FACTOR * (float) Math.sqrt(Float.MAX_VALUE);
    }
    return new Node(initCoords, initDimensions, asLeaf);
  }

  /**
   * @return the maximum number of entries per node
   */
  public int getMaxEntries() {
    return maxEntries;
  }

  /**
   * @return the minimum number of entries per node for all nodes except the root.
   */
  public int getMinEntries() {
    return minEntries;
  }

  /**
   * @return the number of dimensions of the tree
   */
  public int getNumDims() {
    return numDims;
  }

  /**
   * @return the number of items in this tree.
   */
  public int size() {
    return size;
  }

  /**
   * Searches the RTree for objects overlapping with the given rectangle.
   *
   * @param coords the corner of the rectangle that is the lower bound of every dimension (eg. the
   * top-left corner)
   * @param dimensions the dimensions of the rectangle.
   * @return a list of objects whose rectangles overlap with the given rectangle.
   */
  public List<T> search(final float[] coords, final float[] dimensions) {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    LinkedList<T> results = new LinkedList<>();
    search(coords, dimensions, root, results);
    return results;
  }

  @SuppressWarnings("unchecked")
  private void search(final float[] coords, final float[] dimensions, final Node n,
      final LinkedList<T> results) {
    if (n.leaf) {
      for (Node e : n.children) {
        if (isOverlap(coords, dimensions, e.coords, e.dimensions)) {
          results.add(((EntryNode<T>) e).value);
        }
      }
    } else {
      for (Node c : n.children) {
        if (isOverlap(coords, dimensions, c.coords, c.dimensions)) {
          search(coords, dimensions, c, results);
        }
      }
    }
  }

  public List<T> searchWithThreshold(final float[] coords, final float[] dimensions, double thres) {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    LinkedList<T> results = new LinkedList<>();
    searchWithThreshold(coords, dimensions, root, results, thres);
    return results;
  }

  private double mbrED(float[] aCoords, float[] aDims, float[] bCoords, float[] bDims) {
    float dist = 0;
    for (int i = 0; i < aCoords.length; i++) {
      float al = aCoords[i];
      float au = aCoords[i] + aDims[i];
      float bl = bCoords[i];
      float bu = bCoords[i] + bDims[i];
      if (al > bu) {
        dist += (al - bu) * (al - bu);
      } else if (bl > au) {
        dist += (bl - au) * (bl - au);
      }
    }
    return Math.sqrt(dist);
  }

  private void searchWithThreshold(final float[] coords, final float[] dimensions, final Node n,
      final LinkedList<T> results, double threshold) {
    if (n.leaf) {
      for (Node e : n.children) {
        if (mbrED(coords, dimensions, e.coords, e.dimensions) <= threshold) {
          results.add(((EntryNode<T>) e).value);
        }
      }
    } else {
      for (Node c : n.children) {
        if (mbrED(coords, dimensions, c.coords, c.dimensions) <= threshold) {
          searchWithThreshold(coords, dimensions, c, results, threshold);
        }
      }
    }
  }

  /**
   * Deletes the entry associated with the given rectangle from the RTree
   *
   * @param coords the corner of the rectangle that is the lower bound in every dimension
   * @param dimensions the dimensions of the rectangle
   * @param entry the entry to delete
   * @return true iff the entry was deleted from the RTree.
   */
  public boolean delete(final float[] coords, final float[] dimensions, final T entry) {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    Node l = findLeaf(root, coords, dimensions, entry);
    if (l == null) {
      throw new IndexRuntimeException("leaf not found for entry " + entry);
    }
    ListIterator<Node> li = l.children.listIterator();
    T removed = null;
    while (li.hasNext()) {
      @SuppressWarnings("unchecked")
      EntryNode<T> e = (EntryNode<T>) li.next();
      if (e.value.equals(entry)) {
        removed = e.value;
        li.remove();
        break;
      }
    }
    if (removed != null) {
      condenseTree(l);
      size--;
    }
    if (size == 0) {
      root = buildRoot(true);
    }
    return (removed != null);
  }

  public boolean delete(final float[] coords, final T entry) {
    return delete(coords, pointDims, entry);
  }

  private Node findLeaf(final Node n, final float[] coords,
      final float[] dimensions, final T entry) {
    if (n.leaf) {
      for (Node c : n.children) {
        if (((EntryNode) c).value.equals(entry)) {
          return n;
        }
      }
      return null;
    } else {
      for (Node c : n.children) {
        if (isOverlap(c.coords, c.dimensions, coords, dimensions)) {
          Node result = findLeaf(c, coords, dimensions, entry);
          if (result != null) {
            return result;
          }
        }
      }
      return null;
    }
  }

  private void condenseTree(final Node pn) {
    Node n = pn;
    Set<Node> q = new HashSet<>();
    while (n != root) {
      if (n.leaf && (n.children.size() < minEntries)) {
        q.addAll(n.children);
        n.parent.children.remove(n);
      } else if (!n.leaf && (n.children.size() < minEntries)) {
        // probably a more efficient way to do this...
        LinkedList<Node> toVisit = new LinkedList<>(n.children);
        while (!toVisit.isEmpty()) {
          Node c = toVisit.pop();
          if (c.leaf) {
            q.addAll(c.children);
          } else {
            toVisit.addAll(c.children);
          }
        }
        n.parent.children.remove(n);
      } else {
        tighten(n);
      }
      n = n.parent;
    }
    if (root.children.isEmpty()) {
      root = buildRoot(true);
    } else if ((root.children.size() == 1) && (!root.leaf)) {
      root = root.children.get(0);
      root.parent = null;
    } else {
      tighten(root);
    }
    for (Node ne : q) {
      @SuppressWarnings("unchecked")
      EntryNode<T> e = (EntryNode<T>) ne;
      insert(e.coords, e.dimensions, e.value);
    }
    size -= q.size();
  }

  /**
   * Empties the RTree
   */
  public void clear() {
    root = buildRoot(true);
    // let the GC take care of the rest.
  }

  /**
   * Inserts the given entry into the RTree, associated with the given rectangle. When the entry is
   * finally inserted, coords and dimensions will be deep-copied.
   *
   * @param coords the corner of the rectangle that is the lower bound in every dimension
   * @param dimensions the dimensions of the rectangle
   * @param entry the entry to insert
   */
  @SuppressWarnings("unchecked")
  public void insert(final float[] coords, final float[] dimensions, final T entry) {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    EntryNode e = new EntryNode(coords, dimensions, entry);
    Node l = chooseLeaf(root, e);
    l.children.add(e);
    size++;
    e.parent = l;
    if (l.children.size() > maxEntries) {
      Node[] splits = splitNode(l);
      adjustTree(splits[0], splits[1]);
    } else {
      adjustTree(l, null);
    }
  }

  /**
   * Convenience method for inserting a point. When the entry is finally inserted, coords and
   * dimensions will be deep-copied.
   */
  public void insert(final float[] coords, final T entry) {
    insert(coords, pointDims, entry);
  }

  private void adjustTree(final Node n, final Node nn) {
    if (n == root) {
      if (nn != null) {
        // build new root and add children.
        root = buildRoot(false);
        root.children.add(n);
        n.parent = root;
        root.children.add(nn);
        nn.parent = root;
      }
      tighten(root);
      return;
    }
    tighten(n);
    if (nn != null) {
      tighten(nn);
      if (n.parent.children.size() > maxEntries) {
        Node[] splits = splitNode(n.parent);
        adjustTree(splits[0], splits[1]);
      }
    }
    if (n.parent != null) {
      adjustTree(n.parent, null);
    }
  }

  @SuppressWarnings("squid:S2140")
  private Node[] splitNode(final Node n) {
    // this class probably calls "tighten" a little too often.
    //For instance, the call at the end of the "while cc.isEmpty()" loop
    // could be modified and inlined because it's only adjusting for the addition of a single node.
    // Left as-is for now for readability.
    Node[] nn = new Node[]{n, new Node(n.coords, n.dimensions, n.leaf)};
    nn[1].parent = n.parent;
    if (nn[1].parent != null) {
      nn[1].parent.children.add(nn[1]);
    }
    LinkedList<Node> cc = new LinkedList<>(n.children);
    n.children.clear();
    Node[] ss = seedPicker == SeedPicker.LINEAR ? lPickSeeds(cc) : qPickSeeds(cc);
    nn[0].children.add(ss[0]);
    nn[1].children.add(ss[1]);
    tighten(nn);
    while (!cc.isEmpty()) {
      if ((nn[0].children.size() >= minEntries)
          && (nn[1].children.size() + cc.size() == minEntries)) {
        nn[1].children.addAll(cc);
        cc.clear();
        tighten(nn); // Not sure this is required.
        return nn;
      } else if ((nn[1].children.size() >= minEntries)
          && (nn[0].children.size() + cc.size() == minEntries)) {
        nn[0].children.addAll(cc);
        cc.clear();
        tighten(nn); // Not sure this is required.
        return nn;
      }
      Node c = seedPicker == SeedPicker.LINEAR ? lPickNext(cc) : qPickNext(cc, nn);
      Node preferred;
      float e0 = getRequiredExpansion(nn[0].coords, nn[0].dimensions, c);
      float e1 = getRequiredExpansion(nn[1].coords, nn[1].dimensions, c);
      if (e0 < e1) {
        preferred = nn[0];
      } else if (e0 > e1) {
        preferred = nn[1];
      } else {
        float a0 = getArea(nn[0].dimensions);
        float a1 = getArea(nn[1].dimensions);
        if (a0 < a1) {
          preferred = nn[0];
        } else if (e0 > a1) {
          preferred = nn[1];
        } else {
          if (nn[0].children.size() < nn[1].children.size()) {
            preferred = nn[0];
          } else if (nn[0].children.size() > nn[1].children.size()) {
            preferred = nn[1];
          } else {
            preferred = nn[(int) Math.round(Math.random())];
          }
        }
      }
      preferred.children.add(c);
      tighten(preferred);
    }
    return nn;
  }

  // Implementation of Quadratic PickSeeds
  private Node[] qPickSeeds(final LinkedList<Node> nn) {
    Node[] bestPair = new Node[2];
    float maxWaste = -1.0f * Float.MAX_VALUE;
    for (Node n1 : nn) {
      for (Node n2 : nn) {
        if (n1 == n2) {
          continue;
        }
        float n1a = getArea(n1.dimensions);
        float n2a = getArea(n2.dimensions);
        float ja = 1.0f;
        for (int i = 0; i < numDims; i++) {
          float jc0 = Math.min(n1.coords[i], n2.coords[i]);
          float jc1 = Math.max(n1.coords[i] + n1.dimensions[i], n2.coords[i] + n2.dimensions[i]);
          ja *= (jc1 - jc0);
        }
        float waste = ja - n1a - n2a;
        if (waste > maxWaste) {
          maxWaste = waste;
          bestPair[0] = n1;
          bestPair[1] = n2;
        }
      }
    }
    nn.remove(bestPair[0]);
    nn.remove(bestPair[1]);
    return bestPair;
  }

  /**
   * Implementation of QuadraticPickNext
   *
   * @param cc the children to be divided between the new nodes, one item will be removed from this
   * list.
   * @param nn the candidate nodes for the children to be added to.
   */
  private Node qPickNext(final LinkedList<Node> cc, final Node[] nn) {
    float maxDiff = -1.0f * Float.MAX_VALUE;
    Node nextC = null;
    for (Node c : cc) {
      float n0Exp = getRequiredExpansion(nn[0].coords, nn[0].dimensions, c);
      float n1Exp = getRequiredExpansion(nn[1].coords, nn[1].dimensions, c);
      float diff = Math.abs(n1Exp - n0Exp);
      if (diff > maxDiff) {
        maxDiff = diff;
        nextC = c;
      }
    }
    assert (nextC != null) : "No node selected from qPickNext";
    cc.remove(nextC);
    return nextC;
  }

  // Implementation of LinearPickSeeds
  private Node[] lPickSeeds(final LinkedList<Node> nn) {
    Node[] bestPair = new Node[2];
    boolean foundBestPair = false;
    float bestSep = 0.0f;
    for (int i = 0; i < numDims; i++) {
      float dimLb = Float.MAX_VALUE;
      float dimMinUb = Float.MAX_VALUE;
      float dimUb = -1.0f * Float.MAX_VALUE;
      float dimMaxLb = -1.0f * Float.MAX_VALUE;
      Node nMaxLb = null;
      Node nMinUb = null;
      for (Node n : nn) {
        if (n.coords[i] < dimLb) {
          dimLb = n.coords[i];
        }
        if (n.dimensions[i] + n.coords[i] > dimUb) {
          dimUb = n.dimensions[i] + n.coords[i];
        }
        if (n.coords[i] > dimMaxLb) {
          dimMaxLb = n.coords[i];
          nMaxLb = n;
        }
        if (n.dimensions[i] + n.coords[i] < dimMinUb) {
          dimMinUb = n.dimensions[i] + n.coords[i];
          nMinUb = n;
        }
      }
      float sep = (nMaxLb == nMinUb) ? -1.0f
          : Math.abs((dimMinUb - dimMaxLb) / (dimUb - dimLb));
      if (sep >= bestSep) {
        bestPair[0] = nMaxLb;
        bestPair[1] = nMinUb;
        bestSep = sep;
        foundBestPair = true;
      }
    }
    // In the degenerate case where all points are the same, the above
    // algorithm does not find a best pair.  Just pick the first 2
    // children.
    if (!foundBestPair) {
      bestPair = new Node[]{nn.get(0), nn.get(1)};
    }
    nn.remove(bestPair[0]);
    nn.remove(bestPair[1]);
    return bestPair;
  }

  /**
   * Implementation of LinearPickNext
   *
   * @param cc the children to be divided between the new nodes, one item will be removed from this
   * list.
   */
  private Node lPickNext(final LinkedList<Node> cc) {
    return cc.pop();
  }

  private void tighten(final Node... nodes) {
    assert (nodes.length >= 1) : "Pass some nodes to tighten!";
    for (Node n : nodes) {
      assert (!n.children.isEmpty()) : "tighten() called on empty node!";
      float[] minCoords = new float[numDims];
      float[] maxCoords = new float[numDims];
      for (int i = 0; i < numDims; i++) {
        minCoords[i] = Float.MAX_VALUE;
        maxCoords[i] = -Float.MAX_VALUE;

        for (Node c : n.children) {
          // we may have bulk-added a bunch of children to a node (eg. in splitNode),
          // so here we just enforce the child->parent relationship.
          c.parent = n;
          if (c.coords[i] < minCoords[i]) {
            minCoords[i] = c.coords[i];
          }
          if ((c.coords[i] + c.dimensions[i]) > maxCoords[i]) {
            maxCoords[i] = (c.coords[i] + c.dimensions[i]);
          }
        }
      }
      for (int i = 0; i < numDims; i++) {
        // Convert max coords to dimensions
        maxCoords[i] -= minCoords[i];
      }
      System.arraycopy(minCoords, 0, n.coords, 0, numDims);
      System.arraycopy(maxCoords, 0, n.dimensions, 0, numDims);
    }
  }

  private Node chooseLeaf(final Node n, final EntryNode<T> e) {
    if (n.leaf) {
      return n;
    }
    float minInc = Float.MAX_VALUE;
    Node next = null;
    for (Node c : n.children) {
      float inc = getRequiredExpansion(c.coords, c.dimensions, e);
      if (inc < minInc) {
        minInc = inc;
        next = c;
      } else if (inc == minInc) {
        float curArea = 1.0f;
        float thisArea = 1.0f;
        for (int i = 0; i < c.dimensions.length; i++) {
          assert next != null;
          curArea *= next.dimensions[i];
          thisArea *= c.dimensions[i];
        }
        if (thisArea < curArea) {
          next = c;
        }
      }
    }
    assert next != null;
    return chooseLeaf(next, e);
  }

  /**
   * Returns the increase in area necessary for the given rectangle to cover the given entry.
   */
  private float getRequiredExpansion(final float[] coords, final float[] dimensions, final Node e) {
    float area = getArea(dimensions);
    float[] deltas = new float[dimensions.length];
    for (int i = 0; i < deltas.length; i++) {
      if (coords[i] + dimensions[i] < e.coords[i] + e.dimensions[i]) {
        deltas[i] = e.coords[i] + e.dimensions[i] - coords[i] - dimensions[i];
      } else if (coords[i] + dimensions[i] > e.coords[i] + e.dimensions[i]) {
        deltas[i] = coords[i] - e.coords[i];
      }
    }
    float expanded = 1.0f;
    for (int i = 0; i < dimensions.length; i++) {
      area *= dimensions[i] + deltas[i];
    }
    return (expanded - area);
  }

  private float getArea(final float[] dimensions) {
    float area = 1.0f;
    for (float dimension : dimensions) {
      area *= dimension;
    }
    return area;
  }

  private boolean isOverlap(final float[] scoords, final float[] sdimensions,
      final float[] coords, final float[] dimensions) {

    for (int i = 0; i < scoords.length; i++) {
      boolean overlapInThisDimension = false;
      if (scoords[i] == coords[i]) {
        overlapInThisDimension = true;
      } else if (scoords[i] < coords[i]) {
        if (scoords[i] + FUDGE_FACTOR * sdimensions[i] >= coords[i]) {
          overlapInThisDimension = true;
        }
      } else if (scoords[i] > coords[i] && coords[i] + FUDGE_FACTOR * dimensions[i] >= scoords[i]) {
        overlapInThisDimension = true;
      }
      if (!overlapInThisDimension) {
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
  public void serialize(ByteArrayOutputStream outputStream,
      BiConsumer<Integer, OutputStream> biConsumer) throws IOException {
    ReadWriteIOUtils.write(maxEntries, outputStream);
    ReadWriteIOUtils.write(minEntries, outputStream);
    ReadWriteIOUtils.write(numDims, outputStream);
    ReadWriteIOUtils.write(seedPicker.serialize(), outputStream);
    serialize(root, outputStream, biConsumer);
  }

  /**
   * Pre-order traversal
   *
   * @param outputStream serialize to
   */
  @SuppressWarnings("unchecked")
  private void serialize(Node node, ByteArrayOutputStream outputStream,
      BiConsumer<Integer, OutputStream> biConsumer) throws IOException {
    if (node instanceof EntryNode) {
      ReadWriteIOUtils.write(0, outputStream);
    } else if (node.leaf) {
      ReadWriteIOUtils.write(1, outputStream);
    } else {
      ReadWriteIOUtils.write(2, outputStream);
    }
    for (float coord : node.coords) {
      ReadWriteIOUtils.write(coord, outputStream);
    }
    for (float dimension : node.dimensions) {
      ReadWriteIOUtils.write(dimension, outputStream);
    }
    if (node instanceof EntryNode) {
      int idx = ((EntryNode<Integer>) node).value;
      ReadWriteIOUtils.write(idx, outputStream);
      biConsumer.accept(idx, outputStream);
    } else {
      // write child
      ReadWriteIOUtils.write(node.children.size(), outputStream);
      for (Node child : node.children) {
        serialize(child, outputStream, biConsumer);
      }
    }
  }


  public static RTree<Integer> deserialize(ByteBuffer byteBuffer,
      BiConsumer<Integer, ByteBuffer> biConsumer) {
    int maxEntries = ReadWriteIOUtils.readInt(byteBuffer);
    int minEntries = ReadWriteIOUtils.readInt(byteBuffer);
    int numDims = ReadWriteIOUtils.readInt(byteBuffer);
    SeedPicker seedPicker = SeedPicker.deserialize(ReadWriteIOUtils.readShort(byteBuffer));
    RTree<Integer> rTree = new RTree<>(maxEntries, minEntries, numDims, seedPicker);

    deserialize(rTree, null, byteBuffer, biConsumer);
    return rTree;
  }

  /**
   * Pre-order traversal
   *
   * @param byteBuffer deserialize from
   */
  private static void deserialize(RTree rTree, Node parent, ByteBuffer byteBuffer,
      BiConsumer<Integer, ByteBuffer> biConsumer) {
    int leafInt = ReadWriteIOUtils.readInt(byteBuffer);
    float[] coords = new float[rTree.numDims];
    float[] dimensions = new float[rTree.numDims];
    for (int i = 0; i < rTree.numDims; i++) {
      coords[i] = ReadWriteIOUtils.readFloat(byteBuffer);
    }
    for (int i = 0; i < rTree.numDims; i++) {
      dimensions[i] = ReadWriteIOUtils.readFloat(byteBuffer);
    }
    Node node;
    if (leafInt == 0) {
      int idx = ReadWriteIOUtils.readInt(byteBuffer);
      biConsumer.accept(idx, byteBuffer);
      node = new EntryNode<>(coords, dimensions, idx);
    } else {
      node = new Node(coords, dimensions, leafInt == 1);
      int childSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < childSize; i++) {
        deserialize(rTree, node, byteBuffer, biConsumer);
      }
    }
    if (parent == null) {
      rTree.root = node;
    } else {
      parent.children.add(node);
      node.parent = parent;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("maxEntries:%d,", maxEntries));
    sb.append(String.format("minEntries:%d,", minEntries));
    sb.append(String.format("numDims:%d,", numDims));
    sb.append(String.format("seedPicker:%s%n", seedPicker));
    if (root == null) {
      return sb.toString();
    }
    toString(root, 0, sb);
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private void toString(Node node, int depth, StringBuilder sb) {
    for (int i = 0; i < depth; i++) {
      sb.append("--");
    }
    sb.append(node.toString());
    sb.append("\n");
    if (!(node instanceof EntryNode)) {
      for (Node child : node.children) {
        toString(child, depth + 1, sb);
      }
    }
  }

  // CHECKSTYLE:OFF
  static class Node {

    final float[] coords;
    final float[] dimensions;
    final LinkedList<Node> children;
    final boolean leaf;
    Node parent;

    private Node(float[] coords, float[] dimensions, boolean leaf) {
      this.coords = new float[coords.length];
      this.dimensions = new float[dimensions.length];
      System.arraycopy(coords, 0, this.coords, 0, coords.length);
      System.arraycopy(dimensions, 0, this.dimensions, 0, dimensions.length);
      this.leaf = leaf;
      children = new LinkedList<>();
    }

    @Override
    public String toString() {
      return "Node{" +
          "coords=" + Arrays.toString(coords) +
          ", dimensions=" + Arrays.toString(dimensions) +
          ", leaf=" + leaf +
          '}';
    }
  }

  private static class EntryNode<T> extends Node {

    public final T value;

    EntryNode(final float[] coords, final float[] dimensions, final T value) {
      // an entry isn't actually a leaf (its parent is a leaf)
      // but all the algorithms should stop at the first leaf they encounter,
      // so this little hack shouldn't be a problem.
      super(coords, dimensions, true);
      this.value = value;
    }

    @Override
    public String toString() {
      return "Entry: " + value + "," + super.toString();
    }
  }

}