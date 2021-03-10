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
import org.apache.iotdb.db.index.IndexManager;
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

import static org.apache.iotdb.db.index.IndexTestUtils.getArrayRange;
import static org.apache.iotdb.db.index.common.IndexConstant.MODEL_PATH;
import static org.apache.iotdb.db.index.common.IndexType.MMHH;
import static org.junit.Assert.fail;

public class MMHHIndexReadIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";

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
    insertSQL();
  }

  private void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    //    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));
      System.out.println(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      for (int i = 0; i < 1; i++) {
        String wholePath = String.format(directionPattern, i);
        System.out.println(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
      }
      System.out.println(
          String.format(
              "CREATE INDEX ON %s WITH INDEX=%s, SERIES_LENGTH=%d, HASH_LENGTH=%d, %s=%s",
              indexWhole,
              MMHH,
              wholeDim,
              hashLength,
              MODEL_PATH,
              "\"/Users/kangrong/code/github/deep-learning/hash_journal/TAH_project/src/mmhh.pt\""));
      //      String modelPath =
      // "/Users/kangrong/code/github/deep-learning/hash_journal/TAH_project/src/mmhh.pt";
      String modelPath = "src/test/resources/index/mmhh.pt";
      if (!(new File(modelPath).exists())) {
        fail("model file path is not correct!");
      }
      statement.execute(
          String.format(
              "CREATE INDEX ON %s WITH INDEX=%s, SERIES_LENGTH=%d, HASH_LENGTH=%d, %s=%s",
              indexWhole,
              MMHH,
              wholeDim,
              hashLength,
              MODEL_PATH,
              String.format("\"%s\"", modelPath)));
      for (int i = 0; i < wholeSize; i++) {
        String device = String.format(directionDevicePattern, i);
        for (int j = 0; j < wholeDim; j++) {
          String insertSQL =
              String.format(insertPattern, device, directionSensor, j, (i * wholeDim + j) * 1d);
          //          System.out.println(insertSQL);
          statement.execute(insertSQL);
        }
      }
      statement.execute("flush");
      System.out.println(IndexManager.getInstance().getRouter());
      //      IndexManager.getInstance().stop();
      //      IndexManager.getInstance().start();

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

  @Test
  public void checkRead() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String querySQL =
          String.format(
              "SELECT TOP 2 direction FROM root.wind2.* WHERE direction LIKE (%s)",
              getArrayRange(121, 121 + wholeDim));

      System.out.println(querySQL);
      boolean hasIndex = statement.execute(querySQL);
      StringBuilder gt = new StringBuilder();
      gt.append("Time,root.wind2.1.direction.(D=0.00),root.wind2.4.direction.(D=1.00),\n");
      for (int i = 0; i < 100; i++) {
        gt.append(String.format("%d,%.1f,%.1f,\n", i, 100f + i, 400f + i));
      }
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
        Assert.assertEquals(gt.toString(), sb.toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
