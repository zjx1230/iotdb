/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE_ELE;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.iotdb.db.index.algorithm.paa.PAATimeFixedPreprocessor;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexPreprocessorOverlapTest {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %d)";
  private static final String storageGroup = "root.v";
  private static final String p1 = "root.v.d0.p1";
  private static final String device = "root.v.d0";
  private static final String p1s = "p1";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.getConnection
        (Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {

      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroup));
      statement
          .execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=INT32,ENCODING=PLAIN", p1));

      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
              p1, IndexType.ELB, INDEX_WINDOW_RANGE, 5, INDEX_SLIDE_STEP, 3, DISTANCE, L_INFINITY,
              ELB_TYPE, ELB_TYPE_ELE));
      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d",
              p1, IndexType.PAA_INDEX, INDEX_WINDOW_RANGE, 5, INDEX_SLIDE_STEP, 2));

      long i;
      long timeInterval = 0;
      // time partition 1, seq file 1
      for (i = 0; i < 15; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + i);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }


  @Test
  public void indexManagerRestart() {
    // clear all storage group processor from memory
    IndexManager indexManager = IndexManager.getInstance();
    try {
      String gtp1ELB = "{startTime=0, endTime=13, dataSize=294}";
      String gtp1PAA = "{startTime=0, endTime=14, dataSize=414}";
      String gtp1PAAOverlap = "{[12,12],[13,13],[14,14],}";
      indexManager.closeAndClear();
      //reload
      System.out.println("closed!");
      List<IndexChunkMeta> p1ELBChunkMetas = indexManager
          .getIndexSGMetadata(storageGroup, true, p1, IndexType.ELB);
      StringBuilder p1ELB = new StringBuilder();
      p1ELBChunkMetas.forEach(p -> p1ELB.append(p.toStringStable()));
      System.out.println(p1ELB);
      Assert.assertEquals(gtp1ELB, p1ELB.toString());

      List<IndexChunkMeta> p1PAAChunkMetas = indexManager
          .getIndexSGMetadata(storageGroup, true, p1, IndexType.PAA_INDEX);
      StringBuilder p1PAA = new StringBuilder();
      p1PAAChunkMetas.forEach(p -> p1PAA.append(p.toStringStable()));
      System.out.println(p1PAA);
      Assert.assertEquals(gtp1PAA, p1PAA.toString());

      String fakeTsFileName = "9587719150666-1-0.tsfile";
      IndexFileProcessor processor = indexManager
          .getNewIndexFileProcessor(storageGroup, true, 0, fakeTsFileName);
      TVListAllocator.getInstance().allocate(TSDataType.INT32);
      PAATimeFixedPreprocessor preprocessor = new PAATimeFixedPreprocessor(TSDataType.INT32, 10, 3,
          4, 0, true, true);
      preprocessor.deserializePrevious(processor.getPreviousMeta().get(p1).get(IndexType.PAA_INDEX));
      System.out.println(IndexTestUtils.tvListToString(preprocessor.getSrcData()));
      Assert.assertEquals(gtp1PAAOverlap, IndexTestUtils.tvListToString(preprocessor.getSrcData()));

      indexManager.closeAndClear();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
