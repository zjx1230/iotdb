package org.apache.iotdb.db.index.usable;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

public class BasicIndexUsability implements IIndexUsable {

  private final String routerFilePath;

  public BasicIndexUsability(String routerFilePath) {
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
  public void addUsableRange(PartialPath partialPath, long startTime, long endTime) {

  }

  @Override
  public void addUnusableRange(PartialPath partialPath, long startTime, long endTime) {

  }
}
