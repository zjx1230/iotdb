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

import static org.apache.iotdb.db.index.IndexTestUtils.funcForm;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE_ELE;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;
import static org.apache.iotdb.db.index.common.IndexFunc.ED;
import static org.apache.iotdb.db.index.common.IndexFunc.SIM_ET;
import static org.apache.iotdb.db.index.common.IndexFunc.SIM_ST;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBIndexELBReadIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %d)";
  private static final String storageGroup = "root.v";
  private static final String p1 = "root.v.d0.p1";
  private static final String p2 = "root.v.d0.p2";
  private static final String device = "root.v.d0";
  private static final String p1s = "p1";
  private static final String p2s = "p2";
  private static final String TIMESTAMP_STR = "Time";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    insertSQL();
  }

  private static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
//    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection = DriverManager.getConnection
        (Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {

      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroup));
      statement
          .execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=INT32,ENCODING=PLAIN", p1));
      statement
          .execute(String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", p2));
//      statement
//          .execute(String.format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d",
//              p1, IndexType.NO_INDEX, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 5));
      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
              p1, IndexType.ELB, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 5, DISTANCE, L_INFINITY,
              ELB_TYPE, ELB_TYPE_ELE));


      long i;
      long timeInterval = 0;
      int unseqDelta = 1000;
      // time partition 1, seq file 1
      for (i = 0; i < 30; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i * 2));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 1, seq file 1");

      // time partition 2, seq file 2
      timeInterval = 50_000;
      for (i = 0; i < 30; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i * 2));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, seq file 2");

//      // time partition 2, seq file 3
      for (i = 200; i < 230; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i - 200));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i - 200));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, seq file 3");

      // time partition 2, unseq file 1, overlap with seq file 2
      for (i = 10; i < 40; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i * 2 + unseqDelta));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, unseq file 1");

      // time partition 2, seq file 4, unsealed
      for (i = 400; i < 500; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i * 2));
      }
      System.out.println("insert finish time partition 2, seq file 4, unsealed");

      // time partition 2, unseq file 2, overlap with seq file 4
      for (i = 250; i < 300; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, i * 2 + unseqDelta));
      }
      System.out.println("insert finish time partition 2, unseq file 2, unsealed");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /**
   * <p>insert: [0,0]~[0,30], [xxx000,xxx30]~[0,30], [xxx200,xxx230]~[0,30]</p>
   * <p>update: [xxx010,xxx1010]~[xxx040,xxx1040]</p>
   * <p>two unsealed files.</p>
   *
   * <p>query: [10,11,...19], threshold=0</p>
   *
   * <p>Expect: </p>
   */
  @Test
  public void distanceMeasureIT() throws ClassNotFoundException {
//    Thread.sleep(60000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      StringBuilder pattern = new StringBuilder();
      pattern.append("\'");
      for (int i = 10; i < 20; i++) {
        pattern.append(i).append(',');
      }
      pattern.append("\'");
      double threshold = 5;
      String querySQL = String.format(
          "SELECT INDEX sim_st(%s),sim_et(%s),ed(%s) FROM %s WHERE time >= 50 WITH INDEX=ELB, threshold=%s, pattern=%s",
          p1s, p1s, p1s, device, threshold, pattern);
      boolean hasIndex = statement.execute(querySQL);

      Assert.assertTrue(hasIndex);
      String[] gt = {"0,0,50210,0,50219,0,0.0"};
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
              + resultSet.getString("ID0") + ","
              + resultSet.getString(funcForm(SIM_ST, p1)) + ","
              + resultSet.getString("ID1") + ","
              + resultSet.getString(funcForm(SIM_ET, p1)) + ","
              + resultSet.getString("ID2") + ","
              + resultSet.getString(funcForm(ED, p1));
          System.out.println("============" + ans);
          Assert.assertEquals(gt[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(gt.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void distanceMeasureIT2() throws ClassNotFoundException {
//    Thread.sleep(60000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      StringBuilder pattern = new StringBuilder();
      pattern.append("\'");
      for (int i = 10; i < 20; i++) {
        pattern.append(i).append(',');
      }
      pattern.append("\'");
      double threshold = 5;
      boolean hasIndex = statement.execute(String.format(
          "SELECT INDEX sim_st(%s),sim_et(%s),ed(%s) FROM %s WITH INDEX=ELB, threshold=%s, pattern=%s",
          p1s, p1s, p1s, device, threshold, pattern));

      Assert.assertTrue(hasIndex);
      String[] gt = {
          "0,0,10,0,19,0,0.0",
          "0,1,50210,1,50219,1,0.0"};
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
              + resultSet.getString("ID0") + ","
              + resultSet.getString(funcForm(SIM_ST, p1)) + ","
              + resultSet.getString("ID1") + ","
              + resultSet.getString(funcForm(SIM_ET, p1)) + ","
              + resultSet.getString("ID2") + ","
              + resultSet.getString(funcForm(ED, p1));
          System.out.println("============" + ans);
          Assert.assertEquals(gt[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(gt.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
