package org.apache.iotdb.db.index.router;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

public interface IIndexRouter {

//  IndexProcessor getIndexProcessor(PartialPath path);

  /**
   * given a index processor path, justify whether it has been registered in router.
   */
  boolean hasIndexProcessor(String path);


  List<IndexProcessor> getIndexProcessorByStorageGroup(String storageGroupPath);

  void removeIndexProcessorByStorageGroup(String storageGroupPath);

  boolean addIndexIntoRouter(PartialPath prefixPath, IndexInfo indexInfo) throws MetadataException;


  boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType)
      throws MetadataException;

  Map<String, IndexProcessor> getProcessorsByStorageGroup(String storageGroup);
//
//  Map<IndexType, IndexInfo> getAllIndexInfos(String prefixPath);

//  IndexInfo getIndexInfoByPath(PartialPath prefixPath, IndexType indexType);

  Iterable<IndexProcessor> getAllIndexProcessors();

  IndexProcessor getIndexProcessorByPath();

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter() {
      return new ProtoIndexRouter(null);
    }
  }

}
