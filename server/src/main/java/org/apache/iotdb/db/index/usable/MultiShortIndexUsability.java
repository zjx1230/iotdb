package org.apache.iotdb.db.index.usable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

  private final Set<PartialPath> unusableSeriesSet;

  MultiShortIndexUsability() {
    this.unusableSeriesSet = new HashSet<>();
  }


  @Override
  public void addUsableRange(PartialPath fullPath, long start, long end) {
    // do nothing temporarily
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, long start, long end) {
    unusableSeriesSet.add(fullPath);
  }

  @Override
  public Set<PartialPath> getUnusableRange() {
    return Collections.unmodifiableSet(this.unusableSeriesSet);
//    if (prunedResult == null) {
//      return Collections.unmodifiableSet(this.unusableSeriesSet);
//    }
//    MultiShortIndexUsability multiPrunedResult = (MultiShortIndexUsability) prunedResult;
//    multiPrunedResult.unusableSeriesSet.addAll(this.unusableSeriesSet);
//    return multiPrunedResult.unusableSeriesSet;
  }

//  @Override
//  public Set<PartialPath> getAllUnusableSeriesForWholeMatching() {
//    return Collections.unmodifiableSet(usableInfoSet);
//  }
//
//
//  @Override
//  public List<Filter> getUnusableRangeForSubMatching(
//      IIndexUsable cannotPruned) {
//    throw new UnsupportedOperationException();
//  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(unusableSeriesSet.size(), outputStream);
    for (PartialPath s : unusableSeriesSet) {
      ReadWriteIOUtils.write(s.getFullPath(), outputStream);
    }
  }

  @Override
  public void deserialize(InputStream inputStream) throws IllegalPathException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      unusableSeriesSet.add(new PartialPath(ReadWriteIOUtils.readString(inputStream)));
    }
  }
//
//  @Override
//  public void updateELBBlocksForSeriesMatching(PrimitiveList unusableBlocks, List<ELBWindowBlockFeature> windowBlocks) {
//    throw new UnsupportedOperationException();
//  }

//  @Override
//  public IIndexUsable deepCopy() {
//    throw new UnsupportedOperationException();
//  }
}
