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
package org.apache.iotdb.db.index.it;

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
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBIndexIndexManagerIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %d)";
  private static final String storageGroup = "root.v";
  private static final String p1 = "root.v.d0.p1";
  private static final String p2 = "root.v.d0.p2";
  private static final String device = "root.v.d0";
  private static final String p1s = "p1";
  private static final String p2s = "p2";

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
      statement
          .execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=INT32,ENCODING=PLAIN", p2));

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

      long i;
      long timeInterval = 0;
      int unseqDelta = 1000;
      // time partition 1, seq file 1
      for (i = 0; i < 100; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + i);
      // time partition 2, seq file 2
      timeInterval = 1_000_000_000;
      for (i = 0; i < 100; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + (i + timeInterval));
      // time partition 2, seq file 3
      for (i = 200; i < 300; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + (i + timeInterval));

      // time partition 2, unseq file 1, overlap with seq file 2
      for (i = 50; i < 150; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2 + unseqDelta));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + (i + timeInterval));
      // time partition 2, seq file 4, unsealed
      for (i = 400; i < 500; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      System.out.println("================ flush and close: " + (i + timeInterval));
      // time partition 2, unseq file 2, overlap with seq file 4
      for (i = 250; i < 300; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2 + unseqDelta));
      }
      statement.execute("flush");
      System.out.println("================ flush and close: " + (i + timeInterval));
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
      String gtp1ELB = "{startTime=0, endTime=99, dataSize=654}{startTime=1000000000, endTime=1000000099, dataSize=654}{startTime=1000000200, endTime=1000000299, dataSize=654}{startTime=1000000400, endTime=1000000499, dataSize=654}";
      String gtp1PAA = "{startTime=0, endTime=99, dataSize=654}{startTime=1000000000, endTime=1000000099, dataSize=654}{startTime=1000000200, endTime=1000000299, dataSize=654}{startTime=1000000400, endTime=1000000499, dataSize=654}";
      String gtp2ELB = "{startTime=0, endTime=99, dataSize=654}{startTime=1000000000, endTime=1000000099, dataSize=654}{startTime=1000000200, endTime=1000000299, dataSize=654}{startTime=1000000400, endTime=1000000499, dataSize=654}";

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

      List<IndexChunkMeta> p2ELBChunkMetas = indexManager
          .getIndexSGMetadata(storageGroup, true, p2, IndexType.ELB);
      StringBuilder p2ELB = new StringBuilder();
      p2ELBChunkMetas.forEach(p -> p2ELB.append(p.toStringStable()));
      System.out.println(p2ELB);
      Assert.assertEquals(gtp2ELB, p2ELB.toString());
      indexManager.deleteAll();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
