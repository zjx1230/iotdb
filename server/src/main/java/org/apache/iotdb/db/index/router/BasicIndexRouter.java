package org.apache.iotdb.db.index.router;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

public class BasicIndexRouter implements IIndexRouter {

  private final String routerFilePath;

  public BasicIndexRouter(String routerFilePath) {
    this.routerFilePath = routerFilePath;

    File schemaFolder = SystemFileFactory.INSTANCE.getFile(routerFilePath);
//    if (!schemaFolder.exists()) {
//      if (schemaFolder.mkdirs()) {
//        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
//      } else {
//        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
//      }
//    }
//    logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
//    mtreeSnapshotPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
//    mtreeSnapshotTmpPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;
  }

  @Override
  public IndexFileProcessor getIndexProcessor(PartialPath path) {
    return null;
  }

  @Override
  public void setIndexProcessor(PartialPath path, IndexFileProcessor indexFileProcessor) {

  }

  @Override
  public boolean createIndex(List<PartialPath> prefixPaths, IndexInfo indexInfo) {
    return false;
  }

  @Override
  public boolean dropIndex(List<PartialPath> prefixPaths, IndexType indexType) {
    return false;
  }

  @Override
  public Map<IndexType, IndexInfo> getAllIndexInfos(PartialPath prefixPath) {
    return null;
  }

  @Override
  public IndexInfo getIndexInfoByPath(PartialPath prefixPath, IndexType indexType) {
    return null;
  }

  @Override
  public void close() {

  }
}
