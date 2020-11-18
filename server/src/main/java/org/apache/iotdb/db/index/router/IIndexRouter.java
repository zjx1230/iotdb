package org.apache.iotdb.db.index.router;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.index.common.func.IndexNaiveFunc;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

public interface IIndexRouter {

  /**
   * given a index processor path, justify whether it has been registered in router.
   */
  boolean hasIndexProcessor(PartialPath path);

  boolean addIndexIntoRouter(PartialPath prefixPath, IndexInfo indexInfo, CreateIndexProcessorFunc func) throws MetadataException;


  boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType)
      throws MetadataException;


  Iterable<IndexProcessor> getAllIndexProcessors();

  Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path);

  void serialize();

  void deserialize(CreateIndexProcessorFunc func);

  IIndexRouter getRouterByStorageGroup(String storageGroupPath);

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter(String routerDir) {
      return new ProtoIndexRouter(routerDir);
    }
  }

}
