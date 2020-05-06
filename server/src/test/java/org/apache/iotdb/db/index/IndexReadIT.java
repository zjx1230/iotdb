package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE_ELE;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexReadIT {

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

      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
              p1, IndexType.ELB, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10, DISTANCE, L_INFINITY,
              ELB_TYPE, ELB_TYPE_ELE));
      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d",
              p1, IndexType.PAA, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10));
      statement.execute(String
          .format("CREATE INDEX ON %s WHERE time > 0 WITH INDEX=%s, %s=%d, %s=%d, %s=%s, %s=%s",
              p2, IndexType.ELB, INDEX_WINDOW_RANGE, 10, INDEX_SLIDE_STEP, 10, DISTANCE, L_INFINITY,
              ELB_TYPE, ELB_TYPE_ELE));

      long i;
      long timeInterval = 0;
      int unseqDelta = 1000;
      // time partition 1, seq file 1
      for (i = 0; i < 100; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 1, seq file 1");

      // time partition 2, seq file 2
      timeInterval = 1_000_000_000;
      for (i = 0; i < 100; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, seq file 2");

      // time partition 2, seq file 3
      for (i = 200; i < 300; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, seq file 3");

      // time partition 2, unseq file 1, overlap with seq file 2
      for (i = 50; i < 150; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2 + unseqDelta));
      }
      statement.execute("flush");
      System.out.println("insert finish time partition 2, unseq file 1");

      // time partition 2, seq file 4, unsealed
      for (i = 400; i < 500; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2));
      }
      System.out.println("insert finish time partition 2, seq file 4, unsealed");

      // time partition 2, unseq file 2, overlap with seq file 4
      for (i = 250; i < 300; i++) {
        statement.execute(String.format(insertPattern,
            device, p1s, timeInterval + i, timeInterval + i + unseqDelta));
        statement.execute(String.format(insertPattern,
            device, p2s, timeInterval + i, timeInterval + i * 2 + unseqDelta));
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

  @Test
  public void sumAggreWithSingleFilterTest()
      throws ClassNotFoundException, SQLException, InterruptedException {
//    Thread.sleep(60000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      StringBuilder pattern = new StringBuilder();
      pattern.append("\'");
      for (int i = 0; i < 10; i++) {
        pattern.append(i).append(',');
      }
      pattern.append("\'");
      boolean queryIndex = statement.execute(String.format("SELECT INDEX sim_st(%s),sim_et(%s),dist(%s) FROM %s WHERE time >= 50 WITH INDEX=PAA, pattern=%s", p1s, p1s, p2s, device, pattern));

      boolean hasResultSet = statement.execute(String.format("SELECT sum(%s),avg(%s),sum(%s) FROM %s WHERE time >= 50", p1s, p1s, p2s, device));
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(p1))
              + "," + resultSet.getString(sum(p1)) + "," + Math
              .round(resultSet.getDouble(sum(p2)));
          System.out.println("!!!!!============ " + ans);
//          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
//        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
