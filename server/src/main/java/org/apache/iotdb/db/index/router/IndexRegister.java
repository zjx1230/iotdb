//package org.apache.iotdb.db.index.router;
//
//import java.util.List;
//import java.util.Map;
//import org.apache.iotdb.db.exception.metadata.MetadataException;
//import org.apache.iotdb.db.index.common.IndexInfo;
//import org.apache.iotdb.db.index.common.IndexType;
//import org.apache.iotdb.db.metadata.MManager;
//import org.apache.iotdb.db.metadata.PartialPath;
//
//public class IndexRegister {
//
//  public List<PartialPath> createIndex(List<PartialPath> prefixPaths, IndexInfo indexInfo)
//      throws MetadataException {
//    return MManager.getInstance().createIndex(prefixPaths, indexInfo);
//  }
//
//  public List<PartialPath> dropIndex(List<PartialPath> prefixPaths, IndexType indexType)
//      throws MetadataException {
//    return MManager.getInstance().dropIndex(prefixPaths, indexType);
//  }
//
//  public Map<String, Map<IndexType, IndexInfo>> getAllIndexInfosInStorageGroup(
//      String storageGroupName) {
//    return MManager.getInstance().getAllIndexInfosInStorageGroup(storageGroupName);
//  }
//
//  public IndexInfo getIndexInfoByPath(String path, IndexType indexType) {
//    return MManager.getInstance().getIndexInfoByPath(path, indexType);
//  }
//}
