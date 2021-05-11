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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.db.index.common.IndexConstant.MODEL_PATH;
import static org.apache.iotdb.db.index.common.IndexType.MMHH;
import static org.junit.Assert.fail;

public class ShowIndexIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupWhole = "root.wind2";

  private static final String speed1 = "root.wind1.azq01.speed";

  private static final String directionDevicePattern = "root.wind2.%d";
  private static final String directionPattern = "root.wind2.%d.direction";
  private static final String directionSensor = "direction";

  private static final String indexWhole = "root.wind2.*.direction";
  private static final int wholeSize = 20;
  private static final int wholeDim = 100;
  private static final int hashLength = 48;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    //    insertSQL();
  }

  @Test
  public void createIndexBeforeCreateSeries() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    //    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));
      System.out.println(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      System.out.println(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      System.out.println(
          String.format(
              "CREATE INDEX ON %s WITH INDEX=%s, SERIES_LENGTH=%d, HASH_LENGTH=%d, %s=%s",
              indexWhole, MMHH, wholeDim, hashLength, MODEL_PATH, "\"resources/index/mmhh.pt\""));
      String modelPath = "src/test/resources/index/mmhh.pt";
      if (!(new File(modelPath).exists())) {
        fail("model file path is not correct!");
      }
      statement.execute(
          "CREATE INDEX ON root.wind2.*.direction WITH INDEX=MMHH, SERIES_LENGTH=100, "
              + "HASH_LENGTH=48, MODEL_PATH=\"src/test/resources/index/mmhh.pt\"");
      statement.execute(
          "CREATE INDEX ON root.wind1.azq01.speed WITH INDEX=ELB_INDEX, BLOCK_SIZE=5");
      statement.execute(
          "CREATE INDEX ON root.wind2.*.direction WITH INDEX=RTREE_PAA, "
              + "SERIES_LENGTH=15, FEATURE_DIM=4, MAX_ENTRIES=10, MIN_ENTRIES=2");
      statement.execute("flush");
      //      System.out.println(IndexManager.getInstance().getRouter());
      //      IndexManager.getInstance().stop();
      //      IndexManager.getInstance().start();
      checkRead(
          "SHOW INDEX",
          "index series,index type,index attribute,\n"
              + "root.wind1.azq01.speed,ELB_INDEX,BLOCK_SIZE=5,\n"
              + "root.wind2.*.direction,RTREE_PAA,FEATURE_DIM=4,MIN_ENTRIES=2,SERIES_LENGTH=15,MAX_ENTRIES=10,\n"
              + "root.wind2.*.direction,MMHH,MODEL_PATH=src/test/resources/index/mmhh.pt,SERIES_LENGTH=100,HASH_LENGTH=48,\n");

      checkRead(
          "SHOW INDEX FROM root.*",
          "index series,index type,index attribute,\n"
              + "root.wind1.azq01.speed,ELB_INDEX,BLOCK_SIZE=5,\n"
              + "root.wind2.*.direction,RTREE_PAA,FEATURE_DIM=4,MIN_ENTRIES=2,SERIES_LENGTH=15,MAX_ENTRIES=10,\n"
              + "root.wind2.*.direction,MMHH,MODEL_PATH=src/test/resources/index/mmhh.pt,SERIES_LENGTH=100,HASH_LENGTH=48,\n");

      checkRead(
          "SHOW INDEX FROM root.wind2.*",
          "index series,index type,index attribute,\n"
              + "root.wind2.*.direction,RTREE_PAA,FEATURE_DIM=4,MIN_ENTRIES=2,SERIES_LENGTH=15,MAX_ENTRIES=10,\n"
              + "root.wind2.*.direction,MMHH,MODEL_PATH=src/test/resources/index/mmhh.pt,SERIES_LENGTH=100,HASH_LENGTH=48,\n");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }

  private void checkRead(String SQL, String gt) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasIndex = statement.execute(SQL);

      Assert.assertTrue(hasIndex);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          sb.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        sb.append("\n");
        while (resultSet.next()) {
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            sb.append(resultSet.getString(i)).append(",");
          }
          sb.append("\n");
        }
        System.out.println(sb);
        Assert.assertEquals(gt, sb.toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
