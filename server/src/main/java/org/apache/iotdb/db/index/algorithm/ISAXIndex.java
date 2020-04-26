package org.apache.iotdb.db.index.algorithm;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.DataFileInfo;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * ISAXIndex first splits the input data into segments and extracts their discrete SAX features, and
 * then inserts the data features into a root leaf node.  When the number of series of a leaf node
 * reaches the given threshold, ISAXIndex will upscale the resolution of SAX features, thus dividing
 * them into two leaf nodes. The SAX lower bound constraint is used to guarantee no-false-dismissals
 * pruning.<p>
 *
 * In some indexing techniques, ISAXIndex is composed of a list of subtrees.Suppose the input series
 * is divided into {@code b} segments, all data will be first divided into {@code 2^b} sub-index
 * tree according to the first-bits of SAX features.
 */
public class ISAXIndex extends IoTDBIndex {

  public ISAXIndex(IndexInfo indexInfo) {
    super( indexInfo);
  }

  @Override
  public boolean build(Path path, DataFileInfo newFile, Map<String, Object> parameters)
      throws IndexManagerException {
    return false;
  }

  @Override
  public boolean flush(Path path, DataFileInfo newFile, Map<String, Object> parameters)
      throws IndexManagerException {
    return false;
  }

  @Override
  public Object queryByIndex(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals, int limitSize) throws IndexManagerException {
    return null;
  }

  @Override
  public Object queryByScan(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals, int limitSize) throws IndexManagerException {
    return null;
  }
}
