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

import static org.apache.iotdb.db.index.IndexTestUtils.deserializeIndexChunk;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexTestUtils.Validation;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
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
    mManager.setStorageGroup(new PartialPath(storageGroup));
    mManager.createTimeseries(new PartialPath(p1), TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    mManager.createTimeseries(new PartialPath(p2), TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    Map<String, String> props = new HashMap<>();
    props.put(INDEX_WINDOW_RANGE, "5");
    props.put(INDEX_SLIDE_STEP, "5");

    mManager.createIndex(Collections.singletonList(new PartialPath(p1)), new IndexInfo(NO_INDEX, 0, props));
    mManager.createIndex(Collections.singletonList(new PartialPath(p2)), new IndexInfo(NO_INDEX, 0, props));
  }

  private long defaultIndexBufferSize;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    MManager.getInstance().init();
    MManager.getInstance().clear();
    IndexTestUtils.clearIndexFile(tempIndexFileName);
    defaultIndexBufferSize = IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize();
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(100);
    FSFactoryProducer.getFSFactory().getFile(tempIndexFileDir).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    IndexTestUtils.clearIndexFile(tempIndexFileName);
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(defaultIndexBufferSize);
    EnvironmentUtils.cleanEnv();

  }


  @Test
  public void testMultiThreadWrite()
      throws MetadataException, ExecutionException, InterruptedException, IOException {
    prepareMManager();
    // Prepare data
    TVList p1List = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    TVList p2List = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
    for (int i = 0; i < 50; i++) {
      p1List.putInt(i * 2, i * 2);
      p2List.putFloat(i * 3, i * 3);
    }

    String gtStrP1 = "[0-8,5][10-18,5][20-28,5][30-38,5][40-48,5][50-58,5][60-68,5][70-78,5][80-88,5][90-98,5]";
    String gtStrP2 = "[0-12,5][15-27,5][30-42,5][45-57,5][60-72,5][75-87,5][90-102,5][105-117,5][120-132,5][135-147,5]";
    List<Pair<IndexType, String>> gtP1 = new ArrayList<>();
    gtP1.add(new Pair<>(NO_INDEX, gtStrP1));
    List<Pair<IndexType, String>> gtP2 = new ArrayList<>();
    gtP2.add(new Pair<>(NO_INDEX, gtStrP2));

    List<Validation> tasks = new ArrayList<>();
    tasks.add(new Validation(p1, p1List, gtP1));
    tasks.add(new Validation(p2, p2List, gtP2));
    // check result
    checkIndexFlushAndResult(tasks, storageGroup, tempIndexFileDir, tempIndexFileName);
  }


  private static void checkIndexFlushAndResult(List<Validation> tasks, String storageGroup,
      String indexFileDir, String indexFileName)
      throws ExecutionException, InterruptedException, IOException {
    IndexFileProcessor indexFileProcessor = new IndexFileProcessor(storageGroup,
        indexFileName, true, 0, new HashMap<>(), (a, b, c, d) -> {
    });

    indexFileProcessor.startFlushMemTable();
    for (Validation task : tasks) {
      indexFileProcessor.buildIndexForOneSeries(new Path(task.path), task.tvList);
    }
    indexFileProcessor.endFlushMemTable();
    Assert.assertEquals(0, indexFileProcessor.getMemoryUsed().get());
    Assert.assertEquals(0, indexFileProcessor.getNumIndexBuildTasks().get());
    indexFileProcessor.close();
    //read and check
    IndexIOReader reader = new IndexIOReader(indexFileName, false);
    for (Validation task : tasks) {
      for (Pair<IndexType, String> pair : task.gt) {
        IndexType indexType = pair.left;
        System.out.println(String.format("path: %s, index: %s", task.path, indexType));
        String gtDataStr = pair.right;
        List<IndexChunkMeta> metaChunkList = reader.getChunkMetas(task.path, pair.left);

        StringBuilder readStr = new StringBuilder();
        for (IndexChunkMeta chunkMeta : metaChunkList) {
          // data
          ByteBuffer readData = reader.getDataByChunkMeta(chunkMeta);
          readStr.append(deserializeIndexChunk(indexType, readData));
        }
        System.out.println(readStr.toString());
        Assert.assertEquals(gtDataStr, readStr.toString());
      }
    }
  }

}
