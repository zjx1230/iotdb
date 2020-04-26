package org.apache.iotdb.db.index.performance;//

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SharedMemoryTest {

  @Test
  public void testSharedMemory() {
    Object a = PrimitiveArrayPool.getInstance().getDataListsByType(TSDataType.FLOAT, 5);
//    a.putFloat(1, 1);
  }

}
