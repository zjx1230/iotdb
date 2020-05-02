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
package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.PAA;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexFileProcessorTest {

  private static final String storageGroup = "root.vehicle";
  private static final String p1 = "root.vehicle.p1";
  private static final String p2 = "root.vehicle.p2";
  private static final String tempIndexFileDir = "index/root.vehicle/";
  private static final String tempIndexFileName = "index/root.vehicle/demo_index";

  private void prepareMManager() throws MetadataException {
    MManager mManager = MManager.getInstance();
    mManager.init();
    mManager.setStorageGroup(storageGroup);
    mManager.createTimeseries(p1, TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    mManager.createTimeseries(p2, TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    Map<String, String> props = new HashMap<>();
    props.put(INDEX_WINDOW_RANGE, "5");
    props.put(INDEX_SLIDE_STEP, "5");

    mManager.createIndex(Collections.singletonList(p1), new IndexInfo(NO_INDEX, 0, props));
    mManager.createIndex(Collections.singletonList(p1), new IndexInfo(PAA, 0, props));
    mManager.createIndex(Collections.singletonList(p2), new IndexInfo(NO_INDEX, 0, props));
  }

  @Before
  public void setUp() throws Exception {
    MManager.getInstance().init();
    MManager.getInstance().clear();
    EnvironmentUtils.envSetUp();
    TestUtils.clearIndexFile(tempIndexFileName);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    TestUtils.clearIndexFile(tempIndexFileName);
  }


  @Test
  public void testMultiThreadWrite()
      throws SQLException, ClassNotFoundException, MetadataException, ExecutionException, InterruptedException, IOException {
    prepareMManager();
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(100);
    FSFactoryProducer.getFSFactory().getFile(tempIndexFileDir).mkdirs();
    IndexFileProcessor indexFileProcessor = new IndexFileProcessor(storageGroup, tempIndexFileDir,
        tempIndexFileName, true);
    // Prepare data
    TVList p1List = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    TVList p2List = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
    for (int i = 0; i < 50; i++) {
      p1List.putInt(i * 2, i * 2);
      p2List.putFloat(i * 3, i * 3);
    }
    indexFileProcessor.startFlushMemTable();
    indexFileProcessor.buildIndexForOneSeries(new Path(p1), p1List);
    indexFileProcessor.buildIndexForOneSeries(new Path(p2), p2List);
    indexFileProcessor.endFlushMemTable();

    indexFileProcessor.close();
    // check result
  }

}
