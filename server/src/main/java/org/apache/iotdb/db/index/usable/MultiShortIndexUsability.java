package org.apache.iotdb.db.index.usable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.index.algorithm.elb.ELB.ELBWindowBlockFeature;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to record the index usable range for a list of short series, which corresponds to
 * the whole matching scenario.
 *
 * The series path involves wildcard characters. One series is marked as "index-usable" or
 * "index-unusable".
 *
 * It's not thread-safe.
 */
public class MultiShortIndexUsability implements IIndexUsable {

  private static final Logger logger = LoggerFactory.getLogger(MultiShortIndexUsability.class);

  private final PartialPath indexSeries;
  private final Set<PartialPath> usableInfoSet;

  public MultiShortIndexUsability(PartialPath indexSeries) {
    this.indexSeries = indexSeries;
    this.usableInfoSet = new HashSet<>();
  }


  @Override
  public void addUsableRange(PartialPath fullPath, long start, long end) {
    // do nothing temporarily
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, long start, long end) {
    usableInfoSet.add(fullPath);
  }

  @Override
  public Set<PartialPath> getAllUnusableSeriesForWholeMatching() {
    return Collections.unmodifiableSet(usableInfoSet);
  }


  @Override
  public List<Filter> getUnusableRangeForSeriesMatching(
      IIndexUsable cannotPruned) {
    throw new UnsupportedOperationException(indexSeries.toString());
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(usableInfoSet.size(), outputStream);
    for (PartialPath s : usableInfoSet) {
      ReadWriteIOUtils.write(s.getFullPath(), outputStream);
    }
  }

  @Override
  public void deserialize(InputStream inputStream) throws IllegalPathException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      usableInfoSet.add(new PartialPath(ReadWriteIOUtils.readString(inputStream)));
    }
  }

  @Override
  public void updateELBBlocksForSeriesMatching(PrimitiveList unusableBlocks, List<ELBWindowBlockFeature> windowBlocks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IIndexUsable deepCopy() {
    throw new UnsupportedOperationException();
  }
}
