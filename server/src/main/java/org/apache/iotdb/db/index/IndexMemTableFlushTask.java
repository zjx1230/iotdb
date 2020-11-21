package org.apache.iotdb.db.index;

import java.util.Map;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.index.router.ProtoIndexRouter.IndexProcessorStruct;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexMemTableFlushTask is responsible for the index insertion when a TsFileProcessor flushes.
 * IoTDB creates a MemTableFlushTask when a memtable is flushed to disk. After MemTableFlushTask
 * sorts a series, it will be passed to a IndexMemTableFlushTask. The IndexMemTableFlushTask will
 * detect whether one or more indexes have been created on this series, and pass its data to
 * corresponding IndexProcessors and insert it into the corresponding indexes.
 *
 * IndexMemTableFlushTask involves some index processors. It aims to improve the concurrency by
 * partition the index processors.
 */
public class IndexMemTableFlushTask {

//  private static final Logger logger = LoggerFactory.getLogger(IndexMemTableFlushTask.class);
  private final IIndexRouter router;
  //  private final IIndexUsable usability;
  private final boolean sequence;

  /**
   * it should be immutable.
   */
//  private final Map<String, IndexProcessor> processorMap;
  public IndexMemTableFlushTask(IIndexRouter router, boolean sequence
//      ,UpdateIndexFileResourcesCallBack addResourcesCallBack
  ) {
    // check all processors
    this.router = router;
    this.sequence = sequence;
    // in current version, we don't build index for unsequence block
    if (sequence) {
      for (IndexProcessorStruct p : router.getAllIndexProcessorsAndInfo()) {
        p.processor.startFlushMemTable(p.infos);
      }
    }
  }

  public void buildIndexForOneSeries(PartialPath path, TVList tvList) {
    // in current version, we don't build index for unsequence block
    if (sequence) {
      router.getIndexProcessorByPath(path).forEach(p -> p.buildIndexForOneSeries(path, tvList));
    } else {
      router.getIndexProcessorByPath(path).forEach(p -> p.updateUnsequenceData(path, tvList));
    }
  }

  public void endFlush() {
    if (sequence) {
      router.getAllIndexProcessorsAndInfo().forEach(p->p.processor.endFlushMemTable());
    }
  }
}
