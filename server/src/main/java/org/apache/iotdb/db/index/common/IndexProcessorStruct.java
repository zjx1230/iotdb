package org.apache.iotdb.db.index.common;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.metadata.PartialPath;

public class IndexProcessorStruct {

  public IndexProcessor processor;
  public List<PartialPath> storageGroups;
  public Map<IndexType, IndexInfo> infos;

  public IndexProcessorStruct(IndexProcessor processor, List<PartialPath> storageGroups,
      Map<IndexType, IndexInfo> infos) {
    this.processor = processor;
    this.storageGroups = storageGroups;
    this.infos = infos;
  }


  @Override
  public String toString() {
    return "<" + infos + "," + processor + ">";
  }
}
