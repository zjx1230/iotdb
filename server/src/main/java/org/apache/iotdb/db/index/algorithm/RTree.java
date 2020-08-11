package org.apache.iotdb.db.index.algorithm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * A simple implementation of R-Tree, referring to the article: Guttman, Antonin. "R-trees: A
 * dynamic index structure for spatial searching." SIGMOD 1984
 */
public class RTree<T> {

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
        return new RNode[]{node, newNode};
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
    return new RNode[]{node, newNode};
  }

  /**
   * Clear the RTree
   */
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

  /**
   * Insert a rectangle into RTree.
   */
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


  /**
   * Returns the increase in area necessary for the given rectangle to cover the given entry.
   */
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
      if ((n2.lbs[i] - n1.ubs[i] > VAGUE_ERROR) ||
          (n1.lbs[i] - n2.ubs[i] > VAGUE_ERROR)) {
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
    ReadWriteIOUtils.write(dim, outputStream);
    ReadWriteIOUtils.write(nMaxPerNode, outputStream);
    ReadWriteIOUtils.write(nMinPerNode, outputStream);
    ReadWriteIOUtils.write(seedsPicker.serialize(), outputStream);
    serialize(root, outputStream, biConsumer);
  }

  /**
   * Pre-order traversal
   *
   * @param outputStream serialize to
   */
  @SuppressWarnings("unchecked")
  private void serialize(RNode node, ByteArrayOutputStream outputStream,
      BiConsumer<Integer, OutputStream> biConsumer) throws IOException {
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
      int idx = ((Item<Integer>) node).v;
      ReadWriteIOUtils.write(idx, outputStream);
      biConsumer.accept(idx, outputStream);
    } else {
      // write child
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      for (RNode child : node.getChildren()) {
        serialize(child, outputStream, biConsumer);
      }
    }
  }


  public static RTree<Integer> deserialize(ByteBuffer byteBuffer,
      BiConsumer<Integer, ByteBuffer> biConsumer) {
    int dim = ReadWriteIOUtils.readInt(byteBuffer);
    int nMaxPerNode = ReadWriteIOUtils.readInt(byteBuffer);
    int nMinPerNode = ReadWriteIOUtils.readInt(byteBuffer);
    SeedsPicker seedsPicker = SeedsPicker.deserialize(ReadWriteIOUtils.readShort(byteBuffer));
    RTree<Integer> rTree = new RTree<>(nMaxPerNode, nMinPerNode, dim, seedsPicker);
    deserialize(rTree, null, byteBuffer, biConsumer);
    return rTree;
  }

  /**
   * Pre-order traversal
   *
   * @param byteBuffer deserialize from
   */
  private static void deserialize(RTree rTree, RNode parent, ByteBuffer byteBuffer,
      BiConsumer<Integer, ByteBuffer> biConsumer) {
    int nodeType = ReadWriteIOUtils.readInt(byteBuffer);
    float[] lbs = new float[rTree.dim];
    float[] ubs = new float[rTree.dim];
    for (int i = 0; i < rTree.dim; i++) {
      lbs[i] = ReadWriteIOUtils.readFloat(byteBuffer);
    }
    for (int i = 0; i < rTree.dim; i++) {
      ubs[i] = ReadWriteIOUtils.readFloat(byteBuffer);
    }
    RNode node;
    if (nodeType == ITEM) {
      int idx = ReadWriteIOUtils.readInt(byteBuffer);
      biConsumer.accept(idx, byteBuffer);
      node = new Item<>(lbs, ubs, idx);
    } else {
      node = new RNode(lbs, ubs, nodeType == LEAF_NODE);
      int childSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < childSize; i++) {
        deserialize(rTree, node, byteBuffer, biConsumer);
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


  private void toString(RNode node, int depth, StringBuilder sb) {
    for (int i = 0; i < depth; i++) {
      sb.append("--");
    }
    sb.append(node.toString());
    sb.append("\n");
    if (!(node instanceof RTree.Item)) {
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

    public List<RNode> getChildren() {
      return children;
    }

    @Override
    public String toString() {
      return "RNode{" +
          "LB=" + Arrays.toString(lbs) +
          ", UB=" + Arrays.toString(ubs) +
          ", leaf=" + isLeaf +
          '}';
    }

  }

  private static class Item<T> extends RNode {

    private final T v;

    private Item(float[] lbs, float[] ubs, T v) {
      super(lbs, ubs, true);
      this.v = v;
    }

    @Override
    public String toString() {
      return "Item: " + v + "," + super.toString();
    }
  }


  enum SeedsPicker {
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

    /**
     * @return the integer used to determine index type
     */
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

}
