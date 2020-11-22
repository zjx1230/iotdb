package org.apache.iotdb.db.index.usable;

import static org.apache.iotdb.db.index.read.IndexTimeRange.toFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.algorithm.elb.ELB.ELBWindowBlockFeature;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory.getLogger(SingleLongIndexUsability.class);
  /**
   * TODO it will be moved to configuration.
   *
   */
  private static final int defaultSizeOfUsableSegments = 20;

  private int maxSizeOfUsableSegments;
  private RangeNode unusableRanges;
  private int size;
  protected final PartialPath indexSeries;

  SingleLongIndexUsability(PartialPath indexSeries) {
    this(indexSeries, defaultSizeOfUsableSegments);
  }

  SingleLongIndexUsability(PartialPath indexSeries, int maxSizeOfUsableSegments) {
    this.indexSeries = indexSeries;
    this.maxSizeOfUsableSegments = maxSizeOfUsableSegments;
    unusableRanges = new RangeNode(Long.MIN_VALUE, Long.MAX_VALUE, null);
    size = 1;
  }

  @Override
  public void addUsableRange(PartialPath fullPath, long startTime, long endTime) {
    // simplify the problem
    if (startTime == Long.MIN_VALUE) {
      startTime = startTime + 10;
    }
    if (endTime == Long.MAX_VALUE) {
      endTime = endTime - 10;
    }
    RangeNode node = locateIdxByTime(startTime);
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
      } else if (node.start <= startTime && endTime <= node.end) {
        if (node.start == startTime) {
          // left aligned
          node.start = endTime + 1;
          prevNode = node;
          node = node.next;
        } else if (node.end == endTime) {
          // right aligned
          node.end = startTime - 1;
          prevNode = node;
          node = node.next;
        }
        // the unusable range is split. If it reaches the upper bound, not split it
        else if (size < maxSizeOfUsableSegments) {
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
      } else if (startTime < node.start) {
        node.start = endTime + 1;
        prevNode = node;
        node = node.next;
      } else {
        node.end = startTime - 1;
        prevNode = node;
        node = node.next;
      }
    }
  }


  @Override
  public void minusUsableRange(PartialPath fullPath, long startTime, long endTime) {
    // simplify the problem
    if (startTime == Long.MIN_VALUE) {
      startTime = startTime + 10;
    }
    if (endTime == Long.MAX_VALUE) {
      endTime = endTime - 10;
    }
    RangeNode node = locateIdxByTime(startTime);
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

    RangeNode(RangeNode unusableRanges) {
      this.start = unusableRanges.start;
      this.end = unusableRanges.end;
      this.next = unusableRanges.next;
    }

    @Override
    public String toString() {
      return "[" + (start == Long.MIN_VALUE ? "MIN" : start) + "," +
          (end == Long.MAX_VALUE ? "MAX" : end) + "],";
    }

    public static void serialize(RangeNode head, OutputStream outputStream) throws IOException {
      int listLength = 0;
      RangeNode tmp = head;
      while (tmp != null) {
        listLength++;
        tmp = tmp.next;
      }
      ReadWriteIOUtils.write(listLength, outputStream);
      while (head != null) {
        ReadWriteIOUtils.write(head.start, outputStream);
        ReadWriteIOUtils.write(head.end, outputStream);
        head = head.next;
      }
    }

    public static RangeNode deserialize(InputStream inputStream) throws IOException {
      int listLength = ReadWriteIOUtils.readInt(inputStream);
      RangeNode fakeHead = new RangeNode(-1, -1, null);
      RangeNode node = fakeHead;
      for (int i = 0; i < listLength; i++) {
        long start = ReadWriteIOUtils.readLong(inputStream);
        long end = ReadWriteIOUtils.readLong(inputStream);
        RangeNode nextNode = new RangeNode(start, end, null);
        node.next = nextNode;
        node = nextNode;
      }
      return fakeHead.next;
    }
  }

  @Override
  public PartialPath[] getAllUnusableSeriesForWholeMatching(PartialPath indexSeries) {
    throw new UnsupportedOperationException(indexSeries.getFullPath());
  }

  @Override
  public List<Filter> getUnusableRangeForSeriesMatching(IIndexUsable cannotPruned) {
    List<Filter> res = new ArrayList<>();
    SingleLongIndexUsability singleCannotPruned = (SingleLongIndexUsability) cannotPruned;
    // merge them.
    long startTime = Long.MIN_VALUE;
    long endTime = Math.max(unusableRanges.end, singleCannotPruned.unusableRanges.end);
    RangeNode pThis = unusableRanges.next;
    RangeNode pThat = singleCannotPruned.unusableRanges.next;
    if(pThis == null && pThat == null){
      res.add(toFilter(startTime, endTime));
      return res;
    }
    while (pThis != null || pThat != null) {
      if (pThat != null && pThat.start - 1 <= endTime) {
        if (pThat.end > endTime) {
          endTime = pThat.end;
        }
        pThat = pThat.next;
      }
      if (pThis != null && pThis.start - 1 <= endTime) {
        if (pThis.end > endTime) {
          endTime = pThis.end;
        }
        pThis = pThis.next;
      }
      if (endTime == Long.MAX_VALUE) {
        res.add(toFilter(startTime, endTime));
        return res;
      }
      // endTime != MAX_VALUE, means neither pThat or pThis be null.
      assert pThat != null && pThis != null;
      if (endTime + 1 < pThat.start && endTime + 1 < pThis.start) {
        res.add(toFilter(startTime, endTime));
        if (pThis.start > pThat.start) {
          startTime = pThat.start;
          endTime = pThat.end;
          pThat = pThat.next;
        } else {
          startTime = pThis.start;
          endTime = pThis.end;
          pThis = pThis.next;
        }
      }
    }
    return res;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    RangeNode.serialize(unusableRanges, outputStream);

  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    unusableRanges = RangeNode.deserialize(inputStream);
  }

  /**
   * It's a inefficient implementation
   */
  @Override
  public void updateELBBlocksForSeriesMatching(PrimitiveList unusableBlocks,
      List<ELBWindowBlockFeature> windowBlocks) {
    for (int i = 0; i < windowBlocks.size(); i++) {
      ELBWindowBlockFeature block = windowBlocks.get(i);
      RangeNode node = locateIdxByTime(block.startTime);
      if (node.end < block.startTime && block.endTime < node.next.start) {
        unusableBlocks.setBoolean(i, true);
      }
    }
  }

  @Override
  public IIndexUsable deepCopy() {
    SingleLongIndexUsability res = new SingleLongIndexUsability(indexSeries,
        defaultSizeOfUsableSegments);
    res.size = this.size;
    if (this.unusableRanges == null) {
      return res;
    }
    res.unusableRanges = new RangeNode(this.unusableRanges);
    RangeNode pThis = this.unusableRanges;
    RangeNode pRes = res.unusableRanges;
    while (pThis.next != null) {
      pRes.next = new RangeNode(pThis.next);
      pRes = pRes.next;
      pThis = pThis.next;
    }
    return res;
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
