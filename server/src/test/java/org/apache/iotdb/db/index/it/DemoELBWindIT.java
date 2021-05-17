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
import org.apache.iotdb.db.index.IndexTestUtils;
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
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.index.IndexTestUtils.getStringFromList;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.junit.Assert.fail;

public class DemoELBWindIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";
  private static final String insertPattern_show =
      "INSERT INTO %s(timestamp, %s) VALUES (%s, %.3f)";

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupWhole = "root.wind2";

  private static final String speed1 = "root.wind1.azq01.speed";
  private static final String speed1Device = "root.wind1.azq01";
  private static final String speed1Sensor = "speed";

  private static final String directionDevicePattern = "root.wind2.%d";
  private static final String directionPattern = "root.wind2.%d.direction";
  private static final String directionSensor = "direction";

  private static final String indexSub = speed1;
  private static final String indexWhole = "root.wind2.*.direction";
  private static final int wholeSize = 5;
  private static final int wholeDim = 100;
  private static final int subLength = 50;

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

  private static void insertSQL(boolean createTS, boolean flushAtLast, boolean ignoreTimestamp)
      throws ClassNotFoundException {
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
          String wholePath = String.format(directionPattern, i);
          statement.execute(
              String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
          System.out.println(
              String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        }
      }
      statement.execute(
          String.format("CREATE INDEX ON %s WITH INDEX=%s, BLOCK_SIZE=%d", indexSub, ELB_INDEX, 5));
      System.out.println(
          String.format("CREATE INDEX ON %s WITH INDEX=%s, BLOCK_SIZE=%d", indexSub, ELB_INDEX, 5));

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
      for (int i = 0; i < subInput.size(); i++) {
        //        statement.execute(
        //            String.format(
        //                insertPattern,
        //                speed1Device,
        //                speed1Sensor,
        //                subInput.getTime(i),
        //                subInput.getFloat(i)));

        String insertSQL =
            String.format(
                insertPattern_show,
                speed1Device,
                speed1Sensor,
                RpcUtils.formatDatetime(
                    "iso8601",
                    RpcUtils.DEFAULT_TIMESTAMP_PRECISION,
                    ignoreTimestamp ? i : subInput.getTime(i),
                    ZoneId.systemDefault()),
                subInput.getFloat(i));
        statement.execute(insertSQL);
        //        System.out.println(insertSQL);
      }
      if (flushAtLast) {
        statement.execute("flush");
      }
      //      System.out.println("==========================");
      //      System.out.println(IndexManager.getInstance().getRouter());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void checkReadWithoutCreateTS() throws ClassNotFoundException {
    checkRead(true);
  }

  private void checkRead(boolean createTS) throws ClassNotFoundException {
    insertSQL(createTS, true, false);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      List<Float> pattern = loadSubSeries();
      String querySQL =
          "SELECT speed.* FROM root.wind1.azq01 WHERE Speed "
              + String.format("CONTAIN (%s) WITH TOLERANCE 10 ", getStringFromList(pattern, 0, 30))
              + String.format("CONCAT (%s) WITH TOLERANCE 20 ", getStringFromList(pattern, 30, 70))
              + String.format(
                  "CONCAT (%s) WITH TOLERANCE 10 ", getStringFromList(pattern, 70, 100));
      System.out.println(querySQL);
      System.out.println(IndexTestUtils.executeQuerySQL(statement, querySQL, false));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Float> loadSubSeries() throws IOException {
    String queryPath = "/Users/kangrong/tsResearch/tols/JINFENG/d2/out_sub_pattern.csv";
    List<Float> pattern = new ArrayList<>();
    try (BufferedReader csvReader = new BufferedReader(new FileReader(queryPath))) {
      String row;
      while ((row = csvReader.readLine()) != null) {
        float v = Float.parseFloat(row);
        pattern.add(v);
      }
    }
    return pattern;
  }

  @Test
  public void updateELB() throws ClassNotFoundException {
    insertSQL(true, true, true);

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      List<Float> pattern = loadSubSeries();
      String querySQL =
          "SELECT speed.* FROM root.wind1.azq01 WHERE Speed "
              + String.format("CONTAIN (%s) WITH TOLERANCE 10 ", getStringFromList(pattern, 0, 30))
              + String.format("CONCAT (%s) WITH TOLERANCE 20 ", getStringFromList(pattern, 30, 70))
              + String.format(
                  "CONCAT (%s) WITH TOLERANCE 10 ", getStringFromList(pattern, 70, 100));
      System.out.println(querySQL);
      statement.setQueryTimeout(200);
      //      System.out.println(IndexTestUtils.executeQuerySQL(statement, querySQL, false));
      Assert.assertEquals(
          "Time,root.wind1.azq01.speed.1197,root.wind1.azq01.speed.6685,root.wind1.azq01.speed.7595,\n",
          IndexTestUtils.executeQuerySQL(statement, querySQL, true));
      String breakFirstSegment =
          String.format(
              insertPattern_show,
              speed1Device,
              speed1Sensor,
              RpcUtils.formatDatetime(
                  "iso8601", RpcUtils.DEFAULT_TIMESTAMP_PRECISION, 1255L, ZoneId.systemDefault()),
              1000000.);
      statement.execute(breakFirstSegment);
      // before flush, nothing happened
      //      Assert.assertEquals(
      //          "Time,root.wind1.azq01.speed.1417403212000,"
      //              + "root.wind1.azq01.speed.1417434633000,"
      //              + "root.wind1.azq01.speed.1417439612000,\n",
      //          IndexTestUtils.executeQuerySQL(statement, querySQL, true));
      //
      System.out.println(IndexTestUtils.executeQuerySQL(statement, querySQL, false));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
