package org.apache.iotdb.db.index.usable;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * This class is to record the index usable range for a single long series, which corresponds to the
 * subsequence matching scenario.
 *
 * The series path in the initialized parameters must be a full path (without wildcard characters).
 *
 * Specified a series, this class updates the usable and unusable ranges.
 *
 * It's not thread-safe.
 */
public class SingleLongIndexUsability implements IIndexUsable {

  /**
   * it will be moved to configuration.
   */
  private static final int defaultSizeOfUsableSegments = 10;

  private int maxSizeOfUsableSegments;
  private final RangeNode unusableRanges;
  private int size;

  SingleLongIndexUsability(PartialPath indexSeries) {
    this(indexSeries, defaultSizeOfUsableSegments);
  }

  SingleLongIndexUsability(PartialPath indexSeries, int maxSizeOfUsableSegments) {
    this.maxSizeOfUsableSegments = maxSizeOfUsableSegments;
    unusableRanges = new RangeNode(Long.MIN_VALUE, Long.MAX_VALUE, null);
    size = 1;
  }

  @Override
  public void addUsableRange(PartialPath fullPath, TVList tvList) {
    long startTime = tvList.getMinTime();
    long endTime = tvList.getLastTime();
    RangeNode node = locateIdxByTime(tvList.getMinTime());
    RangeNode prevNode = node;
    while (node != null && node.start <= endTime) {
      assert node.start <= node.end;
      assert startTime <= endTime;
      if (node.end < startTime) {
        prevNode = node;
        node = node.next;
      } else if (startTime <= node.start && node.end <= endTime) {
        // the unusable range is covered.
        prevNode.next = node.next;
        node = node.next;
        size--;
      } else if (node.start < startTime && endTime < node.end) {
        // the unusable range is split. If it reaches the upper bound, not split it
        if (size < maxSizeOfUsableSegments) {
          RangeNode newNode = new RangeNode(endTime + 1, node.end, node.next);
          node.end = startTime - 1;
          newNode.next = node.next;
          node.next = newNode;
          prevNode = newNode;
          node = newNode.next;
          size++;
        } else {
          // don't split, thus do nothing.
          prevNode = node;
          node = node.next;
        }
      } else if (startTime < node.start && endTime < node.end) {
        node.start = endTime + 1;
        prevNode = node;
        node = node.next;
      } else if (node.start < startTime && node.end < endTime) {
        node.end = startTime - 1;
        prevNode = node;
        node = node.next;
      }
    }
  }


  @Override
  public void minusUsableRange(PartialPath fullPath, TVList tvList) {
    long startTime = tvList.getMinTime();
    long endTime = tvList.getLastTime();
    RangeNode node = locateIdxByTime(tvList.getMinTime());
    if (endTime <= node.end) {
      return;
    }
    // add the unusable range into the list
    RangeNode newNode;
    if (startTime <= node.end + 1) {
      // overlapping this node
      node.end = endTime;
      newNode = node;
    } else {
      assert node.next != null;
      if (size < maxSizeOfUsableSegments || node.next.start <= endTime + 1) {
        // new node is added but it overlaps with latter node
        newNode = new RangeNode(startTime, endTime, node.next);
        node.next = newNode;
        node = newNode;
        size++;
      } else {
        // we cannot add more range, thus merge it with closer neighbor.
        if (startTime - node.end < node.next.start - endTime) {
          node.end = endTime;
        } else {
          node.next.start = startTime;
        }
        return;
      }
    }
    node = node.next;
    // merge the overlapped list
    while (node != null && node.start <= endTime) {
      assert node.start <= node.end;
      assert startTime <= endTime;
      if (newNode.end < node.end) {
        newNode.end = node.end;
      }
      // delete the redundant node
      newNode.next = node.next;
      node = node.next;
      size--;
    }
  }

  /**
   * Find the latest node whose start time is less than {@code timestamp} A naive scanning search
   * for the linked list. Further optimization is still going on. It returns a node and there is
   * {@code node.start <= timestamp}
   */
  private RangeNode locateIdxByTime(long timestamp) {
    if (unusableRanges.next == null) {
      return unusableRanges;
    }
    RangeNode res = null;
    RangeNode node = unusableRanges;
    while (node.next != null) {
      if (node.start >= timestamp) {
        break;
      }
      res = node;
      node = node.next;
    }
    return res;
  }

  private static class RangeNode {

    long start;
    long end;
    RangeNode next;

    RangeNode(long start, long end, RangeNode next) {
      this.start = start;
      this.end = end;
      this.next = next;
    }

    @Override
    public String toString() {
      return "[" + (start == Long.MIN_VALUE ? "MIN" : start) + "," +
          (end == Long.MAX_VALUE ? "MAX" : end) + "],";
    }
  }

  @Override
  public PartialPath[] getAllUnusableSeriesForWholeMatching(PartialPath indexSeries) {
    throw new UnsupportedOperationException(indexSeries.getFullPath());
  }

  @Override
  public List<Pair<Long, Long>> getUnusableRangeForSeriesMatching(PartialPath indexSeries) {
    return new ArrayList<>();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(String.format("size:%d,", size));
    RangeNode node = unusableRanges;
    while (node != null) {
      sb.append(node.toString());
      node = node.next;
    }
    return sb.toString();
  }
}
