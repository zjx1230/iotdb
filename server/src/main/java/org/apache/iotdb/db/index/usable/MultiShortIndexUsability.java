package org.apache.iotdb.db.index.usable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.router.ProtoIndexRouter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.Pair;
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
}
