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
package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IoTDBSessionSimpleIT {

  private static Logger logger = LoggerFactory.getLogger(IoTDBSessionSimpleIT.class);

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertByBlankStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1 ");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    session.insertRecord(deviceId, 1L, measurements, values);

    String[] expected = new String[] {"root.sg1.d1.s1 "};

    assertFalse(session.checkTimeseriesExists("root.sg1.d1.s1 "));
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries");
    int i = 0;
    while (dataSet.hasNext()) {
      assertEquals(expected[i], dataSet.next().getFields().get(0).toString());
      i++;
    }

    session.close();
  }

  @Test
  public void testInsertPartialTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

    Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 15; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s1", rowIndex, 1L);
      tablet.addValue("s2", rowIndex, 1D);
      tablet.addValue("s3", rowIndex, new Binary("1"));
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    SessionDataSet dataSet = session.executeQueryStatement("select count(*) from root");
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      Assert.assertEquals(15L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(1).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(2).getLongV());
    }
    session.close();
  }

  @Test
  public void testInsertByStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<String> values = new ArrayList<>();
    values.add("1");
    values.add("1.2");
    values.add("true");
    values.add("dad");
    session.insertRecord(deviceId, 1L, measurements, values);

    Set<String> expected = new HashSet<>();
    expected.add(IoTDBDescriptor.getInstance().getConfig().getIntegerStringInferType().name());
    expected.add(IoTDBDescriptor.getInstance().getConfig().getFloatingStringInferType().name());
    expected.add(IoTDBDescriptor.getInstance().getConfig().getBooleanStringInferType().name());
    expected.add(TSDataType.TEXT.name());

    Set<String> actual = new HashSet<>();
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    while (dataSet.hasNext()) {
      actual.add(dataSet.next().getFields().get(3).getStringValue());
    }

    Assert.assertEquals(expected, actual);

    session.close();
  }

  @Test
  public void testInsertWrongPathByStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1..d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<String> values = new ArrayList<>();
    values.add("1");
    values.add("1.2");
    values.add("true");
    values.add("dad");
    try {
      session.insertRecord(deviceId, 1L, measurements, values);
    } catch (Exception e) {
      logger.error("", e);
    }

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    Assert.assertFalse(dataSet.hasNext());

    session.close();
  }

  @Test
  public void testInsertIntoIllegalTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1\n";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<String> values = new ArrayList<>();
    values.add("1");
    values.add("1.2");
    values.add("true");
    values.add("dad");
    try {
      session.insertRecord(deviceId, 1L, measurements, values);
    } catch (Exception e) {
      logger.error("", e);
    }

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    Assert.assertFalse(dataSet.hasNext());

    session.close();
  }

  @Test
  public void testInsertByObjAndNotInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.TEXT);

    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(1.2d);
    values.add("true");
    values.add("dad");
    session.insertRecord(deviceId, 1L, measurements, dataTypes, values);

    Set<String> expected = new HashSet<>();
    expected.add(TSDataType.INT64.name());
    expected.add(TSDataType.DOUBLE.name());
    expected.add(TSDataType.TEXT.name());
    expected.add(TSDataType.TEXT.name());

    Set<String> actual = new HashSet<>();
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    while (dataSet.hasNext()) {
      actual.add(dataSet.next().getFields().get(3).getStringValue());
    }

    Assert.assertEquals(expected, actual);

    session.close();
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException, MetadataException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d1.s2");
    List<TSDataType> tsDataTypes = new ArrayList<>();
    tsDataTypes.add(TSDataType.DOUBLE);
    tsDataTypes.add(TSDataType.DOUBLE);
    List<TSEncoding> tsEncodings = new ArrayList<>();
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);

    List<Map<String, String>> tagsList = new ArrayList<>();
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "v1");
    tagsList.add(tags);
    tagsList.add(tags);

    session.createMultiTimeseries(
        paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, null, null);

    assertTrue(session.checkTimeseriesExists("root.sg1.d1.s1"));
    assertTrue(session.checkTimeseriesExists("root.sg1.d1.s2"));
    MeasurementMNode mNode =
        (MeasurementMNode) MManager.getInstance().getNodeByPath(new PartialPath("root.sg1.d1.s1"));
    assertNull(mNode.getSchema().getProps());

    session.close();
  }

  @Test
  public void testChineseCharacter() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.存储组1";
    String[] devices = new String[] {"设备1.指标1", "设备1.s2", "d2.s1", "d2.指标2"};
    session.setStorageGroup(storageGroup);
    for (String path : devices) {
      String fullPath = storageGroup + TsFileConstant.PATH_SEPARATOR + path;
      session.createTimeseries(fullPath, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    for (String path : devices) {
      for (int i = 0; i < 10; i++) {
        String[] ss = path.split("\\.");
        StringBuilder deviceId = new StringBuilder(storageGroup);
        for (int j = 0; j < ss.length - 1; j++) {
          deviceId.append(TsFileConstant.PATH_SEPARATOR).append(ss[j]);
        }
        String sensorId = ss[ss.length - 1];
        List<String> measurements = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();

        measurements.add(sensorId);
        types.add(TSDataType.INT64);
        values.add(100L);
        session.insertRecord(deviceId.toString(), i, measurements, types, values);
      }
    }

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.存储组1");
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
    }
    Assert.assertEquals(10, count);
    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void createTimeSeriesWithDoubleTicks()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.sg";
    session.setStorageGroup(storageGroup);

    session.createTimeseries(
        "root.sg.\"my.device.with.colon:\".s",
        TSDataType.INT64,
        TSEncoding.RLE,
        CompressionType.SNAPPY);

    final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
    assertTrue(dataSet.hasNext());

    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void createWrongTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.sg";
    session.setStorageGroup(storageGroup);

    try {
      session.createTimeseries(
          "root.sg.d1..s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("", e);
    }

    final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
    assertFalse(dataSet.hasNext());

    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void testDeleteNonExistTimeSeries()
      throws StatementExecutionException, IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    session.insertRecord(
        "root.sg1.d1", 0, Arrays.asList("t1", "t2", "t3"), Arrays.asList("123", "333", "444"));
    try {
      session.deleteTimeseries(Arrays.asList("root.sg1.d1.t6", "root.sg1.d1.t2", "root.sg1.d1.t3"));
    } catch (BatchExecutionException e) {
      assertEquals("Path [root.sg1.d1.t6] does not exist;", e.getMessage());
    }
    assertTrue(session.checkTimeseriesExists("root.sg1.d1.t1"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t2"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t3"));

    session.close();
  }

  @Test
  public void testInsertOneDeviceRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> datatypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    String value = "{\"iMEI\":\"861394053542937\",\"packetData\":\"7E0F861394053542937FD41076134000150603102B070C9B94087084EFDF0825FFFE1F200F3006EE0ED02C70858FDD3F25201C0010F06F6008B203B23E041100004019A40C7405255006A4629804980334163A0ED068283840D3C1FD004D88EB0334257E0FD0A4383D40D302F2004D0CBC033433547F801E004441640E4116435DF0C2681EAC2FA50B14057980AA0176802607D2019A1E31076882681CA029327080E648CE63DAA1A8356954FA1C1F95AEC721593F40C364FA000D94DD033454460FD060593C40A325FB316D70D49AF4397D8E7F4ED7E3B8CC1DA001B3758086CCD0011A342B0768D8501CA071A3F798C68DDE6A1A377AC769DCE88DA779D37F80660EF9019A3BCC0768F6D01EA0F9C379802610FF639A40FCAB6902F11FA709C43F9E6610E1019A436D076816551DA079D4738066D2C9011A4B2C8F093287202CB16C213C805862B9033E805962D98245411EA02E3D1CA0D1847F80C6D3F7011A51C707684CBD1EA049E5FB03F400200A2273082295EF16C203482ADF2D5813E4012A952F7CEB4310030F60AA2EE163F74F09080C0008FC23F14F89AA4BF8D6FD53122000862A8A0848700748007E\",\"rawDataId\":\"3B028613940535429370\",\"serverIp\":\"192.168.35.87\",\"serverPort\":\"15106\",\"sourceIp\":\"39.144.17.57\",\"sourcePort\":\"25398\",\"timestamp\":1622709837103,\"topic\":\"TYP_KTP_Kobelco_Source\"}";
    addLine(
        times,
        measurements,
        datatypes,
        values,
        3L,
        "TY_0001_Raw_Packet",
        "s2",
        TSDataType.TEXT,
        TSDataType.INT32,
        value,
        2);
    session.insertRecordsOfOneDevice("root.raw.08.8000867157042199208", times, measurements, datatypes, values);
    checkResult(session);
    session.close();
  }

  @Test
  public void testInsertOneDeviceRecordsWithOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> datatypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    addLine(
        times,
        measurements,
        datatypes,
        values,
        1L,
        "s4",
        "s5",
        TSDataType.FLOAT,
        TSDataType.BOOLEAN,
        5.0f,
        Boolean.TRUE);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        2L,
        "s2",
        "s3",
        TSDataType.INT32,
        TSDataType.INT64,
        3,
        4L);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        3L,
        "s1",
        "s2",
        TSDataType.INT32,
        TSDataType.INT32,
        1,
        2);

    session.insertRecordsOfOneDevice("root.sg.d1", times, measurements, datatypes, values, true);
    checkResult(session);
    session.close();
  }

  @Test(expected = BatchExecutionException.class)
  public void testInsertOneDeviceRecordsWithIncorrectOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> datatypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    addLine(
        times,
        measurements,
        datatypes,
        values,
        2L,
        "s2",
        "s3",
        TSDataType.INT32,
        TSDataType.INT64,
        3,
        4L);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        3L,
        "s1",
        "s2",
        TSDataType.INT32,
        TSDataType.INT32,
        1,
        2);
    addLine(
        times,
        measurements,
        datatypes,
        values,
        1L,
        "s4",
        "s5",
        TSDataType.FLOAT,
        TSDataType.BOOLEAN,
        5.0f,
        Boolean.TRUE);

    session.insertRecordsOfOneDevice("root.sg.d1", times, measurements, datatypes, values, true);
    checkResult(session);
    session.close();
  }

  private void checkResult(Session session)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg.d1");
    dataSet.getColumnNames();
    Assert.assertArrayEquals(
        dataSet.getColumnNames().toArray(new String[0]),
        new String[] {
          "Time",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d1.s5",
          "root.sg.d1.s1",
          "root.sg.d1.s2"
        });
    Assert.assertArrayEquals(
        dataSet.getColumnTypes().toArray(new String[0]),
        new String[] {
          TSDataType.INT64.toString(),
          TSDataType.INT64.toString(),
          TSDataType.FLOAT.toString(),
          TSDataType.BOOLEAN.toString(),
          TSDataType.INT32.toString(),
          TSDataType.INT32.toString()
        });
    long time = 1L;
    //
    Assert.assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {0, 3, 4});
    Assert.assertEquals(5.0f, record.getFields().get(1).getFloatV(), 0.01);
    Assert.assertEquals(Boolean.TRUE, record.getFields().get(2).getBoolV());

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {1, 2, 3});
    Assert.assertEquals(4L, record.getFields().get(0).getLongV());
    Assert.assertEquals(3, record.getFields().get(4).getIntV());

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {0, 1, 2});
    Assert.assertEquals(1, record.getFields().get(3).getIntV());
    Assert.assertEquals(2, record.getFields().get(4).getIntV());

    Assert.assertFalse(dataSet.hasNext());
    dataSet.closeOperationHandle();
  }

  private void addLine(
      List<Long> times,
      List<List<String>> measurements,
      List<List<TSDataType>> datatypes,
      List<List<Object>> values,
      long time,
      String s1,
      String s2,
      TSDataType s1type,
      TSDataType s2type,
      Object value1,
      Object value2) {
    List<String> tmpMeasurements = new ArrayList<>();
    List<TSDataType> tmpDataTypes = new ArrayList<>();
    List<Object> tmpValues = new ArrayList<>();
    tmpMeasurements.add(s1);
    tmpMeasurements.add(s2);
    tmpDataTypes.add(s1type);
    tmpDataTypes.add(s2type);
    tmpValues.add(value1);
    tmpValues.add(value2);
    times.add(time);
    measurements.add(tmpMeasurements);
    datatypes.add(tmpDataTypes);
    values.add(tmpValues);
  }

  private void assertNulls(RowRecord record, int[] index) {
    for (int i : index) {
      Assert.assertNull(record.getFields().get(i).getDataType());
    }
  }
}
