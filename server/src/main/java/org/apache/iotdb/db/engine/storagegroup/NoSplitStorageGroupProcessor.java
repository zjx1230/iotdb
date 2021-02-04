package org.apache.iotdb.db.engine.storagegroup;

import java.util.HashMap;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;

// 用于传统的 LSM flush , compaction 流程
public class NoSplitStorageGroupProcessor extends StorageGroupProcessor {

  public NoSplitStorageGroupProcessor(String systemDir, String storageGroupName,
      TsFileFlushPolicy fileFlushPolicy)
      throws StorageGroupProcessorException {
    super(systemDir, storageGroupName, fileFlushPolicy);
  }

  @Override
  public void insert(InsertRowPlan insertRowPlan) throws WriteProcessException {
    // reject insertions that are out of ttl
    if (!isAlive(insertRowPlan.getTime())) {
      throw new OutOfTTLException(insertRowPlan.getTime(), (System.currentTimeMillis() - dataTTL));
    }
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject();
    }
    writeLock();
    try {
      // init map
      long timePartitionId = StorageEngine.getTimePartition(insertRowPlan.getTime());

      latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>());
      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertRowPlan, false);

    } finally {
      writeUnlock();
    }
  }

}
