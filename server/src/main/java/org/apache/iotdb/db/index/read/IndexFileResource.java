package org.apache.iotdb.db.index.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.index.io.IndexIOReader.IndexPair;

/**
 * After index flushing, the IndexFileProcessor will wrap its metadata as an IndexFileResource and
 * register it to IndexStorageGroup.
 */
public class IndexFileResource {

  private final IndexIOReader reader;

  public IndexFileResource(Map<String, Map<IndexType, IndexPair>> metaDataMap,
      String indexFileName) throws IOException {
    this.reader = new IndexIOReader(metaDataMap, indexFileName, true);
  }

  public IndexFileResource(String indexFileName) throws IOException {
    this.reader = new IndexIOReader(indexFileName, true);
  }

  public List<IndexChunkMeta> getChunkMetas(String seriesPath, IndexType indexType)
      throws IOException {
    return reader.getChunkMetas(seriesPath, indexType);
  }

  public ByteBuffer getDataByChunkMeta(IndexChunkMeta indexMeta)
      throws IOException {
    return reader.getDataByChunkMeta(indexMeta);
  }
//  class IndexFileComparator implements Comparator<IndexFileResource> {
//
//    public int compare(IndexFileResources i1, IndexFileResources i2) {
//
//      if (s1.cgpa < s2.cgpa)
//        return 1;
//      else if (s1.cgpa > s2.cgpa)
//        return -1;
//      return 0;
//    }
//  }
}
