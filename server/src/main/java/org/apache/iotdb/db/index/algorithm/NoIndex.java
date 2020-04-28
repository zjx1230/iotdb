package org.apache.iotdb.db.index.algorithm;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.DataFileInfo;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * NoIndex do nothing on feature extracting and data pruning. Its index-available range is always
 * empty.
 */

public class NoIndex extends IoTDBIndex {

  public NoIndex( IndexInfo indexInfo) {
    super(indexInfo);
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
