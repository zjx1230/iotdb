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

import static org.apache.iotdb.db.conf.IoTDBConstant.SEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.UNSEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.META_DIR_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.STORAGE_GROUP_INDEXING_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexManagerTest {

  private static final String storageGroup = "root.vehicle";
  private static final String p1 = "root.vehicle.p1";
  private static final String p2 = "root.vehicle.p2";

  private void prepareMManager() throws MetadataException {
    MManager mManager = MManager.getInstance();
    mManager.init();
    mManager.setStorageGroup(new PartialPath(storageGroup));
    mManager.createTimeseries(new PartialPath(p1), TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    Map<String, String> props = new HashMap<>();
    props.put(INDEX_WINDOW_RANGE, "5");
    props.put(INDEX_SLIDE_STEP, "5");

    IndexManager.getInstance().createIndex(Collections.singletonList(new PartialPath(p1)),
            new IndexInfo(NO_INDEX, 0, props));
  }

  private long defaultIndexBufferSize;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void testRecoverData() throws MetadataException, IOException {
    prepareMManager();
    //create file
//    String indexDataSeqDir =
//        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + SEQUENCE_FLODER_NAME;
//    String indexDataUnSeqDir =
//        DirectoryManager.getInstance().getIndexRootFolder() + File.separator
//            + UNSEQUENCE_FLODER_NAME;
//    String metaDirPath =
//        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + META_DIR_NAME;
//    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
//    // create p1: legal storage groups; create p2: illegal storage groups
//    fsFactory.getFile(indexDataSeqDir + File.separator + p1).mkdirs();
//    fsFactory.getFile(indexDataSeqDir + File.separator + p2).mkdirs();
//    fsFactory.getFile(indexDataUnSeqDir + File.separator + p1).mkdirs();
//    fsFactory.getFile(indexDataUnSeqDir + File.separator + p2).mkdirs();
//    fsFactory.getFile(metaDirPath).mkdirs();
//    fsFactory
//        .getFile(metaDirPath + File.separator + p1 + STORAGE_GROUP_INDEXING_SUFFIX)
//        .createNewFile();
//    fsFactory
//        .getFile(metaDirPath + File.separator + p2 + STORAGE_GROUP_INDEXING_SUFFIX)
//        .createNewFile();
//    IndexManager.getInstance().recoverIndexData();
//    Assert.assertTrue(fsFactory.getFile(indexDataSeqDir + File.separator + p1).exists());
//    Assert.assertTrue(fsFactory.getFile(indexDataUnSeqDir + File.separator + p1).exists());
//    Assert.assertTrue(fsFactory
//        .getFile(metaDirPath + File.separator + p1 + STORAGE_GROUP_INDEXING_SUFFIX).exists());
//    Assert.assertFalse(fsFactory.getFile(indexDataSeqDir + File.separator + p2).exists());
//    Assert.assertFalse(fsFactory.getFile(indexDataUnSeqDir + File.separator + p2).exists());
//    Assert.assertFalse(fsFactory
//        .getFile(metaDirPath + File.separator + p2 + STORAGE_GROUP_INDEXING_SUFFIX).exists());
//    FileUtils.deleteDirectory(new File(DirectoryManager.getInstance().getIndexRootFolder()));
  }

}
