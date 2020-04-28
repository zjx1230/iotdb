package org.apache.iotdb.db.index.algorithm;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.DataFileInfo;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * MBRIndex extracts features for input series data and put them into a root leaf node initially.
 * When the size of a leaf node achieve the specified threshold, the leaf node will split. MBRIndex
 * constructs hierarchical inner nodes above leaf nodes according to a lower bounding constraint for
 * guaranteeing no-false-dismissals pruning.
 *
 */

public class MBRIndex extends IoTDBIndex {

  public MBRIndex( IndexInfo indexInfo) {
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

  @Override
  public void delete() {

  }
}
