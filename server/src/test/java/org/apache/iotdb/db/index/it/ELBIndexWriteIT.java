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

import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.math.Randomwalk;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ELBIndexWriteIT {

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
  }


  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }


  @Test
  public void checkWrite() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
//    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection = DriverManager.getConnection
        (Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      System.out.println(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));
      statement.execute(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));

      for (int i = 0; i < wholeSize; i++) {
        String wholePath = String.format(directionPattern, i);
//        System.out.println(String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
      }
      statement.execute(
          String.format("CREATE INDEX ON %s WITH INDEX=%s, BLOCK_SIZE=10", indexSub, ELB_INDEX));

      System.out.println(IndexManager.getInstance().getRouter());
//      Assert.assertEquals(
//          "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]},root.wind2.*.direction: {}>;"
//              + "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]},root.wind1.azq01.speed: {}>;",
//          IndexManager.getInstance().getRouter().toString());

//      TVList subInput = Randomwalk.generateRanWalkTVList(subLength);
      TVList subInput = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
      for (int i = 0; i < subLength; i++) {
        subInput.putDouble(i, i * 10);
      }
      for (int i = 0; i < 23; i++) {
        statement.execute(String.format(insertPattern,
            speed1Device, speed1Sensor, subInput.getTime(i), subInput.getDouble(i)));
      }
      statement.execute("flush");
      System.out.println("insert finish for subsequence case");
      System.out.println(IndexManager.getInstance().getRouter());

      Assert.assertEquals(
          "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {BLOCK_SIZE=10}]},root.wind1.azq01.speed: {ELB_INDEX=[0-9:45.00, 10-19:145.00]}>;",
          IndexManager.getInstance().getRouter().toString());

      for (int i = 23; i < 37; i++) {
        statement.execute(String.format(insertPattern,
            speed1Device, speed1Sensor, subInput.getTime(i), subInput.getDouble(i)));
      }
      statement.execute("flush");
      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {BLOCK_SIZE=10}]},root.wind1.azq01.speed: {ELB_INDEX=[0-9:45.00, 10-19:145.00, 20-29:245.00]}>;",
          IndexManager.getInstance().getRouter().toString());

      IndexManager.getInstance().stop();
      IndexManager.getInstance().start();
      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {BLOCK_SIZE=10}]},root.wind1.azq01.speed: {}>;",
          IndexManager.getInstance().getRouter().toString());
      for (int i = 37; i < subLength; i++) {
        statement.execute(String.format(insertPattern,
            speed1Device, speed1Sensor, subInput.getTime(i), subInput.getDouble(i)));
      }
      statement.execute("flush");
      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {BLOCK_SIZE=10}]},root.wind1.azq01.speed: {ELB_INDEX=[0-9:45.00, 10-19:145.00, 20-29:245.00, 30-39:345.00, 40-49:445.00]}>;",
          IndexManager.getInstance().getRouter().toString());


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
