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
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.db.index.IndexTestUtils.getArrayRange;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.junit.Assert.fail;

public class ELBIndexReadIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";

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
    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }

  private static void insertSQL() throws ClassNotFoundException {

    Class.forName(Config.JDBC_DRIVER_NAME);
    //    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      statement.execute(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));

      for (int i = 0; i < wholeSize; i++) {
        String wholePath = String.format(directionPattern, i);
        //        System.out.println(String.format("CREATE TIMESERIES %s WITH
        // DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
      }
      statement.execute(
          String.format("CREATE INDEX ON %s WITH INDEX=%s, BLOCK_SIZE=%d", indexSub, ELB_INDEX, 5));

      TVList subInput = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
      for (int i = 0; i < subLength; i++) {
        subInput.putDouble(i, i * 10);
      }
      for (int i = 0; i < 20; i++) {
        statement.execute(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
        System.out.println(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
      }
      statement.execute("flush");
      //      System.out.println("==========================");
      //      System.out.println(IndexManager.getInstance().getRouter());

      for (int i = 25; i < 30; i++) {
        statement.execute(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
        System.out.println(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
      }
      statement.execute("flush");
      //      System.out.println("==========================");
      //      System.out.println(IndexManager.getInstance().getRouter());

      IndexManager.getInstance().stop();
      IndexManager.getInstance().start();

      for (int i = 40; i < subLength; i++) {
        statement.execute(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
        System.out.println(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
      }
      statement.execute("flush");
      //      System.out.println("==========================");
      //      System.out.println(IndexManager.getInstance().getRouter());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void checkRead() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String querySQL =
          "SELECT speed.* FROM root.wind1.azq01 WHERE Speed "
              + String.format("CONTAIN (%s) WITH TOLERANCE 0 ", getArrayRange(170, 200, 10))
              + String.format("CONCAT (%s) WITH TOLERANCE 0 ", getArrayRange(250, 300, 10))
              + String.format("CONCAT (%s) WITH TOLERANCE 0 ", getArrayRange(400, 430, 10));
      System.out.println(querySQL);
      boolean hasIndex = statement.execute(querySQL);
      String gt =
          "Time,root.wind1.azq01.speed.17,\n"
              + "0,170.0,\n"
              + "1,180.0,\n"
              + "2,190.0,\n"
              + "3,250.0,\n"
              + "4,260.0,\n"
              + "5,270.0,\n"
              + "6,280.0,\n"
              + "7,290.0,\n"
              + "8,400.0,\n"
              + "9,410.0,\n"
              + "10,420.0,\n";
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
