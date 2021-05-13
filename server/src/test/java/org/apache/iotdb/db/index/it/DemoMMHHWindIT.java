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
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.index.IndexTestUtils.getStringFromList;
import static org.apache.iotdb.db.index.common.IndexConstant.MODEL_PATH;
import static org.apache.iotdb.db.index.common.IndexType.MMHH;
import static org.junit.Assert.fail;

public class DemoMMHHWindIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";
  private static final String insertPattern_show =
      "INSERT INTO %s(timestamp, %s) VALUES (%s, %.3f)";

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupWhole = "root.wind2";

  private static final String speed1 = "root.wind1.azq01.speed";
  private static final String speed1Device = "root.wind1.azq01";
  private static final String speed1Sensor = "speed";

  private static final String speedDevicePattern = "root.wind2.%d";
  private static final String speedPattern = "root.wind2.%d.speed";
  private static final String speedSensor = "speed";

  private static final String indexSub = speed1;
  private static final String indexWhole = "root.wind2.*.speed";
  private static final int wholeSize = 100;
  private static final int wholeDim = 100;
  private static final int PAA_Dim = 4;
  private static final int hashLength = 48;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }

  private static void insertSQL(boolean createTS) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));
      System.out.println(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      System.out.println(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));
      if (createTS) {
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));
        System.out.println(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));
        for (int i = 0; i < wholeSize; i++) {
          String wholePath = String.format(speedPattern, i);
          statement.execute(
              String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
          System.out.println(
              String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        }
      }
      System.out.println(
          String.format(
              "CREATE INDEX ON %s WITH INDEX=%s, SERIES_LENGTH=%d, HASH_LENGTH=%d, %s=%s",
              indexWhole,
              MMHH,
              wholeDim,
              hashLength,
              MODEL_PATH,
              //
              // "\"/Users/kangrong/code/github/deep-learning/hash_journal/TAH_project/src/mmhh.pt\""
              "\"src/test/resources/index/mmhh.pt\""));
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

      TVList subInput = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
      // read base
      String basePath = "/Users/kangrong/tsResearch/tols/JINFENG/d2/out_sub_base.csv";
      int idx = 0;
      try (BufferedReader csvReader = new BufferedReader(new FileReader(basePath))) {
        String row;
        while ((row = csvReader.readLine()) != null) {
          if (idx >= 1) {
            String[] data = row.split(",");
            long t = Long.parseLong(data[0]);
            float v = Float.parseFloat(data[1]);

            subInput.putFloat(t, v);
            //            subInput.putFloat(idx, v);
          }
          idx++;
        }
      }

      //      for (int i = 0; i < subLength; i++) {
      //        subInput.putDouble(i, i * 10);
      //      }
      assert subInput.size() == wholeDim * wholeSize;
      for (int i = 0; i < wholeSize; i++) {
        long startTime = subInput.getTime(i * wholeDim);
        String device = String.format(speedDevicePattern, startTime);
        for (int j = 0; j < wholeDim; j++) {
          String insertSQL =
              String.format(
                  insertPattern_show,
                  device,
                  speedSensor,
                  RpcUtils.formatDatetime(
                      "iso8601",
                      RpcUtils.DEFAULT_TIMESTAMP_PRECISION,
                      subInput.getTime(i * wholeDim + j),
                      ZoneId.systemDefault()),
                  subInput.getFloat(i * wholeDim + j));
          //          System.out.println(insertSQL);
          statement.execute(insertSQL);
        }
      }
      // 有flush或没有flush结果可能不同
      // 有flush，会找桶，桶里的序列放到result list按ed排序，满了直接离开；hamming dist不一定等于ed，所以可能漏掉一些ed比较小的结果
      // 但没有flush，会把uninvolved 序列全加入result list再按ed排序，这样可能会有更优的结果
      //      statement.execute("flush");
      //      System.out.println("==========================");
      //      System.out.println(IndexManager.getInstance().getRouter());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void checkReadWithoutCreateTS() throws ClassNotFoundException {
    checkRead(false);
  }

  private void checkRead(boolean createTS) throws ClassNotFoundException {
    insertSQL(createTS);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String q1Line =
          "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
              + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
              + "20.0,20.0,20.0,20.0,20.0,"
              + "20.5,21.0,21.5,22.0,22.5,23.0,23.5,24.0,24.5,25.0,25.5,26.0,26.5,27.0,"
              + "27.5,28.0,28.5,29.0,29.5,30.0,29.5,29.0,28.5,28.0,27.5,27.0,26.5,26.0,"
              + "25.5,25.0,24.5,24.0,23.5,23.0,22.5,22.0,21.5,21.0,20.5,"
              + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
              + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0";
      String querySQL =
          String.format("SELECT TOP 3 speed FROM root.wind2.* WHERE speed LIKE (%s)", q1Line);
      System.out.println(querySQL);
      statement.setQueryTimeout(200);
      boolean hasIndex = statement.execute(querySQL);
      //      String gt = "Time,root.wind1.azq01.speed.17,\n";
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
        //        Assert.assertEquals(gt, sb.toString());
      }
      // ========================================
      String basePath = "/Users/kangrong/tsResearch/tols/JINFENG/d2/out_sub_base.csv";
      List<Float> base = new ArrayList<>();
      try (BufferedReader csvReader = new BufferedReader(new FileReader(basePath))) {
        String row;
        int idx = 0;
        while ((row = csvReader.readLine()) != null) {
          if (idx >= 1) {
            String[] data = row.split(",");
            long t = Long.parseLong(data[0]);
            float v = Float.parseFloat(data[1]);
            base.add(v);
          }
          idx++;
        }
      }
      String q2Line = getStringFromList(base, 7600, 7600 + 100);
      querySQL =
          String.format("SELECT TOP 3 speed FROM root.wind2.* WHERE speed LIKE (%s)", q2Line);
      System.out.println(querySQL);
      statement.setQueryTimeout(200);
      hasIndex = statement.execute(querySQL);
      //      String gt = "Time,root.wind1.azq01.speed.17,\n";
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
        //        Assert.assertEquals(gt, sb.toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
