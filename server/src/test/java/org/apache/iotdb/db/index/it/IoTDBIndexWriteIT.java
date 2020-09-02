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
package org.apache.iotdb.db.index.it;

import static org.apache.iotdb.db.index.IndexTestUtils.deserializeIndexChunk;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE_ELE;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;
import static org.apache.iotdb.db.index.common.IndexType.ELB;
import static org.apache.iotdb.db.index.common.IndexType.PAA_INDEX;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.index.IndexTestUtils.Validation;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBIndexWriteIT {

  private static final String storageGroup = "root.v";
  private static final String p1 = "root.v.p1";
  private static final String p2 = "root.v.p2";
  private static final String p1Sensor = "p1";
  private static final String p2Sensor = "p2";
  private long defaultIndexBufferSize;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    defaultIndexBufferSize = IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize();
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(1000);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(defaultIndexBufferSize);
    EnvironmentUtils.cleanEnv();
  }

  private void prepareMManager(Statement statement) throws SQLException {
    statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroup));
    statement.execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=INT32,ENCODING=PLAIN", p1));
    statement.execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=INT32,ENCODING=PLAIN", p2));
    statement.execute(String
        .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
            p1, IndexType.ELB, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10, DISTANCE, L_INFINITY,
            ELB_TYPE, ELB_TYPE_ELE));
    statement.execute(String
        .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d",
            p1, IndexType.PAA_INDEX, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10));
    statement.execute(String
        .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
            p2, IndexType.ELB, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10, DISTANCE, L_INFINITY,
            ELB_TYPE, ELB_TYPE_ELE));
  }

  private String loadIndexFile() {
    String indexRootDir = DirectoryManager.getInstance().getIndexRootFolder();
    //get index file path
    String indexParentDir = String.format("%s/sequence/%s/0", indexRootDir, storageGroup);
    String[] subFileList = new File(indexParentDir).list();
    String indexFile = indexParentDir;
    assert subFileList != null;
    for (String s : subFileList) {
      if (s.endsWith(INDEXED_SUFFIX)) {
        indexFile += "/" + s;
        break;
      }
    }
    return indexFile;
  }

  @Test
  public void testIndexWriteSQLAndReadFile() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()) {

      prepareMManager(statement);
      for (int i = 0; i < 100; i++) {
        statement.execute(String.format("INSERT INTO %s(timestamp, %s) VALUES (%d, %d)",
            storageGroup, p1Sensor, i * 2, i * 2));
        statement.execute(String.format("INSERT INTO %s(timestamp, %s) VALUES (%d, %d)",
            storageGroup, p2Sensor, i * 3, i * 3));
      }
      statement.execute("flush");

      // TODO
      //  this is an interval test for verifying the writing correctness.
      // Finally it will be replaced by SQL QUERY
      Thread.sleep(500);
      String indexFile = loadIndexFile();

      String gtStrP1ELB = ""
          + "(0,[0-18,10])(1,[20-38,10])(2,[40-58,10])(3,[60-78,10])(4,[80-98,10])(5,[100-118,10])(6,[120-138,10])(7,[140-158,10])(8,[160-178,10])(9,[180-198,10])";
      String gtStrP1PAA = ""
          + "(0,[0-9,5])(1,[10-19,5])(2,[20-29,5])(3,[30-39,5])(4,[40-49,5])(5,[50-59,5])(6,[60-69,5])(7,[70-79,5])(8,[80-89,5])(9,[90-99,5])(10,[100-109,5])(11,[110-119,5])(12,[120-129,5])(13,[130-139,5])(14,[140-149,5])(15,[150-159,5])(16,[160-169,5])(17,[170-179,5])(18,[180-189,5])";
      String gtStrP2ELB = ""
          + "(0,[0-27,10])(1,[30-57,10])(2,[60-87,10])(3,[90-117,10])"
          + "(4,[120-147,10])(5,[150-177,10])(6,[180-207,10])(7,[210-237,10])"
          + "(8,[240-267,10])(9,[270-297,10])";
      List<Pair<IndexType, String>> gtP1 = new ArrayList<>();
      gtP1.add(new Pair<>(ELB, gtStrP1ELB));
      gtP1.add(new Pair<>(PAA_INDEX, gtStrP1PAA));
      List<Pair<IndexType, String>> gtP2 = new ArrayList<>();
      gtP2.add(new Pair<>(ELB, gtStrP2ELB));

      List<Validation> tasks = new ArrayList<>();
      tasks.add(new Validation(p1, null, gtP1));
      tasks.add(new Validation(p2, null, gtP2));
      // check result
      checkIndexFlushAndResult(tasks, indexFile);
      System.out.println("finished!");
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }


  private static void checkIndexFlushAndResult(List<Validation> tasks, String indexFileName)
      throws IOException {
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
