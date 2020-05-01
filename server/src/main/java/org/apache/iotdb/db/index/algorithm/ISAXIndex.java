package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.index.common.IndexInfo;

/**
 * ISAXIndex first splits the input data into segments and extracts their discrete SAX features, and
 * then inserts the data features into a root leaf node.  When the number of series of a leaf node
 * reaches the given threshold, ISAXIndex will upscale the resolution of SAX features, thus dividing
 * them into two leaf nodes. The SAX lower bound constraint is used to guarantee no-false-dismissals
 * pruning.<p>
 *
 * In some indexing techniques, ISAXIndex is composed of a list of subtrees.Suppose the input series
 * is divided into {@code b} segments, all data will be first divided into {@code 2^b} sub-index
 * tree according to the first-bits of SAX features.<p>
 *
 * TODO To be implemented.<p>
 */
public abstract class ISAXIndex extends IoTDBIndex {

  public ISAXIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo);
  }
}
