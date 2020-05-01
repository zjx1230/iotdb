package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.index.common.IndexInfo;

/**
 * MBRIndex extracts features for input series data and put them into a root leaf node initially.
 * When the size of a leaf node achieve the specified threshold, the leaf node will split. MBRIndex
 * constructs hierarchical inner nodes above leaf nodes according to a lower bounding constraint for
 * guaranteeing no-false-dismissals pruning.<p>
 *
 * TODO To be implemented.<p>
 */

public abstract class MBRIndex extends IoTDBIndex {

  public MBRIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo);
  }


}
