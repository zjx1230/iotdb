package org.apache.iotdb.db.index.router;

import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;

import java.io.IOException;
import java.util.Map;

public interface IIndexRouter {

  boolean addIndexIntoRouter(
      PartialPath prefixPath,
      IndexInfo indexInfo,
      CreateIndexProcessorFunc func,
      boolean doSerialize)
      throws MetadataException;

  boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType)
      throws MetadataException, IOException;

  Map<IndexType, IndexInfo> getIndexInfosByIndexSeries(PartialPath indexSeries);

  Iterable<IndexProcessorStruct> getAllIndexProcessorsAndInfo();

  Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path);

  void serialize(boolean doClose);

  /**
   * deserialize all index information and processors into the memory
   *
   * @param func
   */
  void deserializeAndReload(CreateIndexProcessorFunc func);

  IIndexRouter getRouterByStorageGroup(String storageGroupPath);

  int getIndexNum();

  /**
   * Index Register validation.
   *
   * @param partialPath
   * @param indexType
   * @param context
   * @return
   * @throws QueryIndexException
   */
  IndexProcessorStruct startQueryAndCheck(
      PartialPath partialPath, IndexType indexType, QueryContext context)
      throws QueryIndexException;

  void endQuery(PartialPath indexProcessor, IndexType indexType, QueryContext context);

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter(String routerDir) {
      return new ProtoIndexRouter(routerDir);
    }
  }
}
