package org.apache.iotdb.db.index.algorithm;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.DataFileInfo;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Hash lookup table is a data structure a structure that can map keys to values. A hash table uses
 * a hash function to compute an index, also called a hash code, into an array of buckets, from
 * which the desired value can be found. Generally, cost on retrieving a bucket can be regarded as
 * {@code O(1)}.<p>
 *
 * Hamming space retrieval returns data points within a Hamming ball of radius {@code H} for each
 * query. Therefore, it enables constant-time Approximate Nearest Neighbor (ANN) search through hash
 * lookups. <p>
 *
 * Based on the mature hash lookup algorithm and the Hamming space retrieval, a rich line of hash
 * methods focuses on better feature representations for preserving the similarity relationship in
 * the Hamming space.
 */
public class HammingHashIndex extends IoTDBIndex {

  public HammingHashIndex(IndexInfo indexInfo) {
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
