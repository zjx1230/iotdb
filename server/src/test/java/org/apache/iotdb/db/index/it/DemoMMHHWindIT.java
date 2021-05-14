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
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.index.IndexManager;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;

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

  private void executeNonQuery(String sql) {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
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
  public void checkReadWithoutCreateTS() throws ClassNotFoundException, StartupException {
    insertSQL(false);
    System.out.println("<<<<<<< Query after insert");
    // MMHH is approximate index. Before flushing, the index hasn't been built thus the query is
    // turned to scan and sort by euclidean distance.
    // Therefore, the first query result might be slightly different from the later three queries.
    checkReads(false);
    executeNonQuery("flush");
    System.out.println("<<<<<<< Query after insert + flush");
    checkReads(true);
    IndexManager.getInstance().stop();
    IndexManager.getInstance().start();
    System.out.println("<<<<<<< Query after stop and restart");
    checkReads(true);
    executeNonQuery("flush");
    System.out.println("<<<<<<< Query after stop and restart + flush");
    checkReads(true);
  }

  private void checkReads(boolean assertResult) throws ClassNotFoundException {
    String template = "SELECT TOP 3 speed FROM root.wind2.* WHERE speed LIKE (%s)";
    String q1Line =
        "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
            + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
            + "20.0,20.0,20.0,20.0,20.0,"
            + "20.5,21.0,21.5,22.0,22.5,23.0,23.5,24.0,24.5,25.0,25.5,26.0,26.5,27.0,"
            + "27.5,28.0,28.5,29.0,29.5,30.0,29.5,29.0,28.5,28.0,27.5,27.0,26.5,26.0,"
            + "25.5,25.0,24.5,24.0,23.5,23.0,22.5,22.0,21.5,21.0,20.5,"
            + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,"
            + "20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0,20.0";
    String q2Best =
        "17.548,16.949,16.906,16.993,17.630,16.945,17.284,16.831,16.706,17.216,"
            + "16.937,16.196,15.916,15.947,15.200,16.076,16.000,16.152,15.537,15.362,"
            + "15.737,15.656,15.995,15.698,15.857,15.706,16.643,16.779,16.383,17.665,"
            + "18.364,19.835,18.765,18.451,19.949,19.530,20.692,20.544,20.879,20.904,"
            + "21.953,21.726,22.571,23.718,24.009,23.462,23.782,23.871,24.508,24.553,"
            + "24.843,24.750,25.500,25.127,25.079,24.951,24.683,24.003,22.977,21.877,"
            + "21.850,21.100,20.499,19.988,19.376,18.960,19.471,17.961,17.570,18.652,"
            + "18.555,18.401,18.645,17.587,18.056,18.448,17.901,18.105,17.168,18.083,"
            + "18.283,16.916,17.099,17.115,18.685,17.483,16.859,16.968,16.339,17.476,"
            + "16.835,16.379,17.312,17.789,17.147,17.456,18.596,18.269,17.956,17.851";
    String q3_76 =
        "18.735,18.299,19.216,18.049,18.762,18.739,18.745,18.074,18.398,17.306,"
            + "17.455,17.401,16.886,16.683,16.413,16.469,17.458,17.429,17.450,16.701,"
            + "16.932,16.032,16.755,16.264,17.173,16.238,16.335,15.200,16.120,16.209,"
            + "18.333,18.754,18.621,18.898,19.394,21.456,20.956,22.844,23.484,23.219,"
            + "23.903,25.306,24.586,25.023,24.670,25.361,25.771,26.625,27.457,27.110,"
            + "26.849,26.500,26.552,26.922,26.056,25.468,25.656,25.830,25.439,25.058,"
            + "23.967,24.251,22.794,22.174,21.551,20.847,20.373,20.662,19.614,19.460,"
            + "18.355,17.562,18.276,18.907,18.524,18.557,18.104,18.487,18.885,19.310,"
            + "19.461,19.036,19.356,19.737,19.328,19.652,19.566,19.629,18.953,18.839,"
            + "18.754,19.221,19.215,19.359,19.324,19.729,19.575,19.720,19.591,19.761";
    String gt_q1_line =
        "Time,root.wind2.1417439639000.speed.(D_Ham=0),root.wind2.1417437998000.speed.(D_Ham=0),root.wind2.1417403228000.speed.(D_Ham=0),\n";
    String gt_q2_best =
        "Time,root.wind2.1417403228000.speed.(D_Ham=0),root.wind2.1417439639000.speed.(D_Ham=0),root.wind2.1417434715000.speed.(D_Ham=0),\n";
    String gt_q3_series76 =
        "Time,root.wind2.1417439639000.speed.(D_Ham=0),root.wind2.1417403228000.speed.(D_Ham=0),root.wind2.1417437998000.speed.(D_Ham=0),\n";
    checkRead(String.format(template, q1Line), assertResult ? gt_q1_line : null, true);
    checkRead(String.format(template, q2Best), assertResult ? gt_q2_best : null, true);
    checkRead(String.format(template, q3_76), assertResult ? gt_q3_series76 : null, true);
  }

  private void checkRead(String querySQL, String gt, boolean onlyHeader)
      throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      //      System.out.println(querySQL);
      //      statement.setQueryTimeout(200);
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
        if (!onlyHeader) {
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              sb.append(resultSet.getString(i)).append(",");
            }
            sb.append("\n");
          }
        }
        System.out.println(">>>>>>>>> Query Result:");
        System.out.println(sb);
        if (gt != null) {
          Assert.assertEquals(gt, sb.toString());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
