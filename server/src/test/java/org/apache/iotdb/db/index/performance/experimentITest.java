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
package org.apache.iotdb.db.index.performance;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.common.math.Randomwalk;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class experimentITest {

  //  private static SessionPool sessionPool = new SessionPool("127.0.0.1", 6667, "root", "root",
  // 10);
  private static AtomicLong totalPoints;
  private static PrimitiveList list;

  private static final int DEVICE_NUM = 1;
  //  private static final int DEVICE_NUM = 2;
  private static final int MEASUREMENT_NUM = 50;
  //  private static final int MEASUREMENT_NUM = 50000;
  private static final int TOTAL_POINTS = 100_000;
  //  private static final int TOTAL_POINTS = 100_000_000;

  private Connection connection;
  private Statement statement;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    this.connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    this.statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }

  private boolean useIndex = true;

  @Test
  public void test() throws InterruptedException, SQLException {

    statement.execute("SET STORAGE GROUP TO root.sg1");
    totalPoints = new AtomicLong();
    list = Randomwalk.generateRanWalk(TOTAL_POINTS);

    List<Thread> threadList = new ArrayList<>();

    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < MEASUREMENT_NUM; j++) {
        if (j % 100 == 0) {
          System.out.println("create sensor: " + j);
        }
        // create timeseries
        //          sessionPool.createTimeseries(
        //              "root.sg1.d" + i + ".s" + j,
        //              TSDataType.DOUBLE,
        //              TSEncoding.GORILLA,
        //              CompressionType.SNAPPY);
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s WITH DATATYPE=DOUBLE,ENCODING=GORILLA, COMPRESSION=SNAPPY",
                "root.sg1.d" + i + ".s" + j));
        // create index
        //          sessionPool.executeNonQueryStatement(
        //              "CREATE INDEX ON root.sg1.d" + i + ".s" + j + " WITH INDEX=ELB_INDEX,
        // BLOCK_SIZE=5");
        if (useIndex) {
          statement.execute(
              String.format(
                  "CREATE INDEX ON %s WITH INDEX=ELB_INDEX, BLOCK_SIZE=5",
                  "root.sg1.d" + i + ".s" + j));
        }
      }
    }
    System.out.println("========= create finish");
    Thread.sleep(500);
    long start = System.nanoTime();
    for (int i = 0; i < DEVICE_NUM; i++) {
      Thread thread = new Thread(new WriteThread(i));
      threadList.add(thread);
      thread.start();
    }

    for (Thread thread : threadList) {
      thread.join();
    }

    long time = (System.nanoTime() - start) / 1000_000_000;
    System.out.println("Average speed: " + totalPoints.get() / (time + 1));
    System.out.println();
    System.out.println("use index: " + useIndex);
    statement.execute("flush");
    query();
  }

  private static String arrayToString(PrimitiveList p) {
    StringBuilder array = new StringBuilder().append(p.getDouble(0));
    for (int i = 1; i < p.size(); i++) {
      array.append(',').append(p.getDouble(i));
    }
    return array.toString();
  }

  private void query() throws SQLException {
    PrimitiveList part1 = Randomwalk.generateRanWalk(20, 1, 0, 1);
    PrimitiveList part2 = Randomwalk.generateRanWalk(20, 3, 0, 1);
    PrimitiveList part3 = Randomwalk.generateRanWalk(20, 5, 0, 1);
    String querySQL =
        "SELECT s1.* FROM root.sg1.d0 WHERE s1 "
            + String.format("CONTAIN (%s) WITH TOLERANCE 5 ", arrayToString(part1))
            + String.format("CONCAT (%s) WITH TOLERANCE 5 ", arrayToString(part2))
            + String.format("CONCAT (%s) WITH TOLERANCE 5 ", arrayToString(part3));
    System.out.println(querySQL);
    boolean hasIndex = statement.execute(querySQL);
    if (!hasIndex) {
      System.out.println("no index result!");
      return;
    }
    try (ResultSet resultSet = statement.getResultSet()) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        sb.append(resultSetMetaData.getColumnName(i)).append(",");
      }
      sb.append("\n");
      int maxShowLines = 5;
      int line = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          sb.append(resultSet.getString(i)).append(",");
        }
        sb.append("\n");
        line++;
        if (line >= maxShowLines) {
          break;
        }
      }
      System.out.println(sb);
    }
  }

  //  private final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";
  class WriteThread implements Runnable {

    int device;

    WriteThread(int device) {
      this.device = device;
    }

    @Override
    public void run() {
      long time = 0;
      while (totalPoints.get() <= TOTAL_POINTS) {
        long start = System.nanoTime();

        time += 1;
        String deviceId = "root.sg1.d" + device;
        //        List<String> measurements = new ArrayList<>();
        //        for (int i = 0; i < MEASUREMENT_NUM; i++) {
        //          measurements.add("s" + i);
        //        }
        StringBuilder measurementsSQL = new StringBuilder();
        for (int i = 0; i < MEASUREMENT_NUM; i++) {
          measurementsSQL.append(",").append("s").append(i);
        }
        //        List<String> values = new ArrayList<>();
        //        for (int i = 0; i < MEASUREMENT_NUM; i++) {
        //          values.add(String.valueOf(list.getDouble((int) time)));
        //        }
        StringBuilder valuesSQL = new StringBuilder();
        for (int i = 0; i < MEASUREMENT_NUM; i++) {
          valuesSQL.append(",").append(String.format("%.3f", list.getDouble((int) time)));
        }
        String insertSQL =
            String.format(
                "INSERT INTO %s(timestamp%s) VALUES (%d%s)",
                deviceId, measurementsSQL.toString(), time, valuesSQL.toString());
        //        System.out.println(insertSQL);
        try {
          statement.execute(insertSQL);
        } catch (SQLException e) {
          e.printStackTrace();
        }
        //        try {
        //          sessionPool.insertRecord(deviceId, time, measurements, values);
        //        } catch (IoTDBConnectionException | StatementExecutionException e) {
        //          e.printStackTrace();
        //        }
        totalPoints.getAndAdd(MEASUREMENT_NUM);
        float writeRate = (float) totalPoints.get() / TOTAL_POINTS;
        //        System.out.println(
        //            Thread.currentThread().getName()
        //                + " write "
        //                + MEASUREMENT_NUM
        //                + " cost: "
        //                + (System.nanoTime() - start) / 1000_000
        //                + "ms, "
        //                + String.format("finish %.1f%%", writeRate * 100));
      }
      //      try {
      //        //        sessionPool.executeNonQueryStatement("flush");
      //        statement.execute("flush");
      //        System.out.println("flushed");
      //      } catch (SQLException e) {
      //        e.printStackTrace();
      //      }
    }
  }
}
