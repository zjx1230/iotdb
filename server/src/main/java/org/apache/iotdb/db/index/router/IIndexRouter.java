package org.apache.iotdb.db.index.router;

import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.index.common.func.IndexNaiveFunc;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

public interface IIndexRouter {

  /**
   * given a index processor path, justify whether it has been registered in router.
   */
  boolean hasIndexProcessor(PartialPath path);

  boolean addIndexIntoRouter(PartialPath prefixPath, IndexInfo indexInfo, CreateIndexProcessorFunc func) throws MetadataException;


  boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType)
      throws MetadataException;


  Iterable<Pair<Map<IndexType, IndexInfo>, IndexProcessor>> getAllIndexProcessorsAndInfo();

  Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path);

  void serializeAndClose();

  /**
   * deserialize all index information and processors into the memory
   * @param func
   */
  void deserializeAndReload(CreateIndexProcessorFunc func);

  IIndexRouter getRouterByStorageGroup(String storageGroupPath);

  int getIndexNum();

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter(String routerDir) {
      return new ProtoIndexRouter(routerDir);
    }
  }

}
