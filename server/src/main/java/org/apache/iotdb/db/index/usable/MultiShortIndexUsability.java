package org.apache.iotdb.db.index.usable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.Pair;

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

  private final PartialPath indexSeries;
  private final Set<PartialPath> usableInfoSet;

  public MultiShortIndexUsability(PartialPath indexSeries) {
    this.indexSeries = indexSeries;
    this.usableInfoSet = new HashSet<>();
  }


  @Override
  public void addUsableRange(PartialPath fullPath, TVList tvList) {
    // do nothing temporarily
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, TVList tvList) {
    usableInfoSet.add(fullPath);
  }

  @Override
  public PartialPath[] getAllUnusableSeriesForWholeMatching(PartialPath indexSeries) {
    return usableInfoSet.toArray(new PartialPath[0]);
  }

  @Override
  public List<Pair<Long, Long>> getUnusableRangeForSeriesMatching(PartialPath indexSeries) {
    throw new UnsupportedOperationException(indexSeries.getFullPath());
  }
}
