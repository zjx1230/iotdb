package org.apache.iotdb.db.index.router;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

public interface IIndexRouter {

  IndexFileProcessor getIndexProcessor(PartialPath path);

  void setIndexProcessor(PartialPath path, IndexFileProcessor indexFileProcessor);

  boolean createIndex(List<PartialPath> prefixPaths, IndexInfo indexInfo) throws MetadataException;

  boolean dropIndex(List<PartialPath> prefixPaths, IndexType indexType) throws MetadataException;

  Map<IndexType, IndexInfo> getAllIndexInfos(PartialPath prefixPath);

  IndexInfo getIndexInfoByPath(PartialPath prefixPath, IndexType indexType);

  void close();

  public static class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter() {
      return new BasicIndexRouter(null);
    }
  }

}
