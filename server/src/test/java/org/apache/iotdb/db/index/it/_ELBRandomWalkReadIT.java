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

import static org.apache.iotdb.db.index.IndexTestUtils.getArrayRange;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.math.Randomwalk;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class _ELBRandomWalkReadIT {

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
    try (Connection connection = DriverManager.getConnection
        (Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      statement.execute(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));

      statement.execute(
          String.format("CREATE INDEX ON %s WITH INDEX=%s, BLOCK_SIZE=%d", indexSub, ELB_INDEX, 5));

      TVList subInput = Randomwalk.generateRanWalkTVList(200);
      long startInsertSub = System.currentTimeMillis();
      for (int i = 0; i < subInput.size(); i++) {
        System.out.println(String.format(insertPattern, speed1Device, speed1Sensor, subInput.getTime(i), subInput.getDouble(i)));
        statement.execute(String.format(insertPattern,
            speed1Device, speed1Sensor, subInput.getTime(i), subInput.getDouble(i)));
      }
      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }


  private String getSubPa(TVList tvList, int offset, int length) {
    StringBuilder sb = new StringBuilder(String.format("%.3f", tvList.getDouble(offset)));
    for (int i = 1; i < length; i++) {
      sb.append(String.format(",%.3f", tvList.getDouble(offset + i)));
    }
    return sb.toString();
  }


  @Test
  public void checkRead() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      TVList subInput = Randomwalk.generateRanWalkTVList(200);
      int offset = 0;
      String querySQL = "SELECT speed.* FROM root.wind1.azq01 WHERE Speed "
          + String.format("CONTAIN (%s) WITH TOLERANCE 0.6 ", getSubPa(subInput, offset, 10))
          + String.format("CONCAT (%s) WITH TOLERANCE 1.3 ", getSubPa(subInput, offset + 10, 30))
          + String
          .format("CONCAT (%s) WITH TOLERANCE 0.9 ", getSubPa(subInput, offset + 40, 10));
      System.out.println(querySQL);
      boolean hasIndex = statement.execute(querySQL);
      String[] gt = {"0,0,50210,0,50219,0,0.0"};
      Assert.assertTrue(hasIndex);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder sb = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            sb.append(resultSet.getString(i)).append(",");
          }
          System.out.println(sb);
//          Assert.assertEquals(gt[cnt], builder.toString());
          cnt++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }


}
