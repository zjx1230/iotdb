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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.session.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.IOException;
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
    session =
        new Session.Builder().host(LOCAL_HOST).port(6667).username("root").password("root").build();
    session.open(false);

    //    try {
    //      session.setStorageGroup("root.sg1");
    //    } catch (StatementExecutionException e) {
    //      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
    //        throw e;
    //      }
    //    }

    // createTemplate();
    //    createTimeseries();
    //    createMultiTimeseries();
    //    insertRecord();
    //    long start = System.currentTimeMillis();
    insertTablet();
    //    System.out.println(System.currentTimeMillis() - start);
    //    insertTabletWithNullValues();
    //    insertTablets();
    //    insertRecords();
    //    selectInto();
    //    createAndDropContinuousQueries();

    //    session.executeNonQueryStatement("flush");

    //    nonQuery();
    //    query();
    //    queryWithTimeout();
    //    rawDataQuery();
    //    lastDataQuery();
    //    queryByIterator();
    //    deleteData();
    //    deleteTimeseries();
    //    setTimeout();

    //    sessionEnableRedirect = new Session(LOCAL_HOST, 6667, "root", "root");
    //    sessionEnableRedirect.setEnableQueryRedirection(true);
    //    sessionEnableRedirect.open(false);
    //
    //    // set session fetchSize
    //    sessionEnableRedirect.setFetchSize(10000);
    //
    //    insertRecord4Redirect();
    //    query4Redirect();
    //    sessionEnableRedirect.close();

    //    long start = System.currentTimeMillis();
    //    session.executeNonQueryStatement(
    //        "select s1,s2,s3 into root.sg2.d1.s1,root.sg2.d1.s2,root.sg2.d1.s3"
    //            + " from root.sg1.d1 where time>=2021-11-25T09:22:35.999+08:00 and time <=
    // 2021-12-26T23:09:15.989+08:00");
    //    System.out.println(System.currentTimeMillis() - start);

    //    long start = System.currentTimeMillis();
    //    session.executeNonQueryStatement(args[0]);
    //            "select sin(s1),sin(s2),sin(s3) into root.sg8.d1.s1,root.sg9.d1.s2,root.sg10.d1.s3
    // from root.sg1.d1 where time>=2021-11-25T09:22:35.999+08:00 and time <=
    // 2021-12-26T23:09:15.989+08:00");
    //    System.out.println(System.currentTimeMillis() - start);
    //
    //    java -cp sql.jar org.apache.iotdb.SessionExample "select s1 into root.t1.d1.s1 from
    // root.s1.d1 where time>=2021-11-25T09:22:35.999+08:00 and time <=
    // 2021-12-26T23:09:15.989+08:00"
    //    java -cp sql.jar org.apache.iotdb.SessionExample "select s1,s2,s3 into root.ss1.d1.s1,
    // root.ss2.d1.s2, root.ss3.d1.s3 from root.sg1.d1 where time>=2021-11-25T09:22:35.999+08:00 and
    // time <= 2021-12-26T23:09:15.989+08:00"
    session.close();
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    //    long s = System.currentTimeMillis();
    //    SessionDataSet dataSet = session.executeQueryStatement("select s1, s2, s3 from
    // root.sg1.d1");
    //    while (dataSet.hasNext()) {
    //      dataSet.next();
    //    }
    //    dataSet.close();
    //    System.out.println(System.currentTimeMillis() - s);

    //    long s = System.currentTimeMillis();
    //    SessionDataSet dataSet =
    //        session.executeQueryStatement("select en(s1), en(s2), en(s3) from root.sg1.d1");
    //    while (dataSet.hasNext()) {
    //      dataSet.next();
    //    }
    //    dataSet.close();
    //    System.out.println(System.currentTimeMillis() - s);
  }

  private static void createAndDropContinuousQueries()
      throws StatementExecutionException, IoTDBConnectionException {
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(s1) INTO temperature_max FROM root.sg1.* "
            + "GROUP BY time(10s) END");
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(s2) INTO temperature_cnt FROM root.sg1.* "
            + "GROUP BY time(10s), level=1 END");
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 20s FOR 20s "
            + "BEGIN SELECT avg(s3) INTO temperature_avg FROM root.sg1.* "
            + "GROUP BY time(10s), level=1 END");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq1");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq2");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq3");
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
      attributes.put("description", "v1");
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

  private static void createTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    Template template = new Template("template1", false);
    MeasurementNode mNodeS1 =
        new MeasurementNode("s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS2 =
        new MeasurementNode("s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS3 =
        new MeasurementNode("s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    template.addToTemplate(mNodeS1);
    template.addToTemplate(mNodeS2);
    template.addToTemplate(mNodeS3);

    session.createSchemaTemplate(template);
    session.setSchemaTemplate("template1", "root.sg1");
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
        typesList.clear();
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
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.DOUBLE));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.DOUBLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.DOUBLE));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    //    for (long row = 0; row < 2_5920_0000; row++) {
    for (long row = 0; row < 2_5920_0000; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        double value = new Random().nextDouble();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp += 10;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTabletWithNullValues()
      throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1,   s2,   s3
     * 1,   null, 1,    1
     * 2,   2,    null, 2
     * 3,   3,    3,    null
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    tablet.bitMaps = new BitMap[schemaList.size()];
    for (int s = 0; s < 3; s++) {
      tablet.bitMaps[s] = new BitMap(tablet.getMaxRowNumber());
    }

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        // mark null value
        if (row % 3 == s) {
          tablet.bitMaps[s].mark((int) row);
        }
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
    BitMap[] bitMaps = new BitMap[schemaList.size()];
    for (int s = 0; s < 3; s++) {
      bitMaps[s] = new BitMap(tablet.getMaxRowNumber());
    }
    tablet.bitMaps = bitMaps;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        // mark null value
        if (row % 3 == i) {
          bitMaps[i].mark(row);
        }
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
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));

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

  private static void nonQuery() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1)");
  }

  private static void setTimeout() throws StatementExecutionException {
    Session tempSession = new Session(LOCAL_HOST, 6667, "root", "root", 10000, 20000);
    tempSession.setQueryTimeout(60000);
  }

  private static void createClusterSession() throws IoTDBConnectionException {
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add("127.0.0.1:6669");
    nodeList.add("127.0.0.1:6667");
    nodeList.add("127.0.0.1:6668");
    Session clusterSession = new Session(nodeList, "root", "root");
    clusterSession.open();
    clusterSession.close();
  }
}
