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
package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@SuppressWarnings("squid:S106")
public class SessionExample {

  private static Session session;
  private static Session sessionEnableRedirect;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1_S5 = "root.sg1.d1.s5";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args)
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
    session.close();
  }

  private static void addLine(
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

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      session.createTimeseries(
          ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S2)) {
      session.createTimeseries(
          ROOT_SG1_D1_S2, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S3)) {
      session.createTimeseries(
          ROOT_SG1_D1_S3, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S4)) {
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "v1");
      Map<String, String> attributes = new HashMap<>();
      tags.put("description", "v1");
      session.createTimeseries(
          ROOT_SG1_D1_S4,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          null,
          tags,
          attributes,
          "temperature");
    }

    // create timeseries with SDT property, SDT will take place when flushing
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S5)) {
      // COMPDEV is required
      // COMPMAXTIME and COMPMINTIME are optional and their unit is ms
      Map<String, String> props = new HashMap<>();
      props.put("LOSS", "sdt");
      props.put("COMPDEV", "0.01");
      props.put("COMPMINTIME", "2");
      props.put("COMPMAXTIME", "10");
      session.createTimeseries(
          ROOT_SG1_D1_S5,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          props,
          null,
          null,
          null);
    }
  }

  private static void createMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists("root.sg1.d2.s1")
        && !session.checkTimeseriesExists("root.sg1.d2.s2")) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d2.s1");
      paths.add("root.sg1.d2.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.INT64);
      tsDataTypes.add(TSDataType.INT64);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);

      List<Map<String, String>> tagsList = new ArrayList<>();
      Map<String, String> tags = new HashMap<>();
      tags.put("unit", "kg");
      tagsList.add(tags);
      tagsList.add(tags);

      List<Map<String, String>> attributesList = new ArrayList<>();
      Map<String, String> attributes = new HashMap<>();
      attributes.put("minValue", "1");
      attributes.put("maxValue", "100");
      attributesList.add(attributes);
      attributesList.add(attributes);

      List<String> alias = new ArrayList<>();
      alias.add("weight1");
      alias.add("weight2");

      session.createMultiTimeseries(
          paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, attributesList, alias);
    }
  }

  private static void insertRecord() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  private static void insertRecord4Redirect()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < 6; i++) {
      for (int j = 0; j < 2; j++) {
        String deviceId = "root.redirect" + i + ".d" + j;
        List<String> measurements = new ArrayList<>();
        measurements.add("s1");
        measurements.add("s2");
        measurements.add("s3");
        List<TSDataType> types = new ArrayList<>();
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);

        for (long time = 0; time < 5; time++) {
          List<Object> values = new ArrayList<>();
          values.add(1L + time);
          values.add(2L + time);
          values.add(3L + time);
          session.insertRecord(deviceId, time, measurements, types, values);
        }
      }
    }
  }

  private static void insertStrRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  private static void insertRecordInObject()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      session.insertRecord(deviceId, time, measurements, types, 1L, 1L, 1L);
    }
  }

  private static void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
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

    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTablets() throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG1_D1, schemaList, 100);
    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);
    Tablet tablet3 = new Tablet("root.sg1.d3", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG1_D1, tablet1);
    tabletMap.put("root.sg1.d2", tablet2);
    tabletMap.put("root.sg1.d3", tablet3);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;
    long[] timestamps3 = tablet3.timestamps;
    Object[] values3 = tablet3.values;

    for (long time = 0; time < 100; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);

        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }
  }

  private static void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = ROOT_SG1_D1_S1;
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    session.deleteTimeseries(paths);
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void query4Redirect()
      throws IoTDBConnectionException, StatementExecutionException {
    String selectPrefix = "select * from root.redirect";
    for (int i = 0; i < 6; i++) {
      SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(selectPrefix + i + ".d1");
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }

      dataSet.closeOperationHandle();
    }

    for (int i = 0; i < 6; i++) {
      SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix + i + ".d1 where time >= 1 and time < 10");
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }

      dataSet.closeOperationHandle();
    }

    for (int i = 0; i < 6; i++) {
      SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix + i + ".d1 where time >= 1 and time < 10 align by device");
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }

      dataSet.closeOperationHandle();
    }

    for (int i = 0; i < 6; i++) {
      SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix
                  + i
                  + ".d1 where time >= 1 and time < 10 and root.redirect"
                  + i
                  + ".d1.s1 > 1");
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }

      dataSet.closeOperationHandle();
    }
  }

  private static void queryWithTimeout()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1", 2000);
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void rawDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    long startTime = 10L;
    long endTime = 200L;

    SessionDataSet dataSet = session.executeRawDataQuery(paths, startTime, endTime);
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024);
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();
  }

  private static void queryByIterator()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    DataIterator iterator = dataSet.iterator();
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (iterator.next()) {
      StringBuilder builder = new StringBuilder();
      // get time
      builder.append(iterator.getLong(1)).append(",");
      // get second column
      if (!iterator.isNull(2)) {
        builder.append(iterator.getLong(2)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get third column
      if (!iterator.isNull(ROOT_SG1_D1_S2)) {
        builder.append(iterator.getLong(ROOT_SG1_D1_S2)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get forth column
      if (!iterator.isNull(4)) {
        builder.append(iterator.getLong(4)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get fifth column
      if (!iterator.isNull(ROOT_SG1_D1_S4)) {
        builder.append(iterator.getObject(ROOT_SG1_D1_S4));
      } else {
        builder.append("null");
      }

      System.out.println(builder);
    }

    dataSet.closeOperationHandle();
  }

  private static void nonQuery() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1)");
  }

  private static void setTimeout() throws StatementExecutionException {
    Session tempSession = new Session(LOCAL_HOST, 6667, "root", "root", 10000, 20000);
    tempSession.setTimeout(60000);
  }
}
