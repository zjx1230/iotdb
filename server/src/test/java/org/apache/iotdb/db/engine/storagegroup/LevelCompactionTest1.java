/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LevelCompactionTest1 {

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private StorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    processor = new DummySGP(systemDir, storageGroup);
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    MergeManager.getINSTANCE().stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSplitSG() throws Exception {
    insertAndSyncClose(1, 10);
    insertAndSyncClose(11, 20);

    insertAndSyncClose(1, 5);
    insertAndSyncClose(11, 15);
    insertAndSyncClose(15, 19);
  }

  private void insertAndSyncClose(int start, int end) throws Exception {
    TSRecord record;
    for (int j = start; j <= end; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    processor.syncCloseAllWorkingTsFileProcessors();

    while (processor.isCompactionMergeWorking()) {
      Thread.sleep(1000);
    }
  }

  class DummySGP extends StorageGroupProcessor {

    DummySGP(String systemInfoDir, String storageGroupName) throws StorageGroupProcessorException {
      super(systemInfoDir, storageGroupName, new TsFileFlushPolicy.DirectFlushPolicy());
    }

  }
}