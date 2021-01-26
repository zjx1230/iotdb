package org.apache.iotdb.db.index.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.metadata.PartialPath;

public class IndexProcessorStruct {

  public IndexProcessor processor;
  public PartialPath representativePath;
  public Map<IndexType, IndexInfo> infos;

  public IndexProcessorStruct(IndexProcessor processor, PartialPath representativePath,
      Map<IndexType, IndexInfo> infos) {
    this.processor = processor;
    this.representativePath = representativePath;
    this.infos = infos;
  }

  public List<StorageGroupProcessor> addMergeLock() throws StorageEngineException {
    return StorageEngine.getInstance().mergeLock(Collections.singletonList(representativePath));
  }


  @Override
  public String toString() {
    return "<" + infos + "\n" + processor + ">";
  }
}
