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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.metadataIndex.BPlusTreeNode;
import org.apache.iotdb.tsfile.file.metadata.metadataIndex.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.metadataIndex.MetadataIndexType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.FileGenerator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** test for BPlusTreeIndexConstructor */
public class BPlusTreeIndexConstructorTest {
  private final TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("BPlusTreeIndexConstructorTest.tsfile");

  private static final String measurementPrefix = "sensor_";
  private static final String vectorPrefix = "vector_";
  private int maxDegreeOfIndexNode;
  private MetadataIndexType metadataIndexType;

  @Before
  public void before() {
    maxDegreeOfIndexNode = conf.getMaxDegreeOfIndexNode();
    conf.setMaxDegreeOfIndexNode(10);

    metadataIndexType = conf.getMetadataIndexType();
    conf.setMetadataIndexType(MetadataIndexType.B_PLUS_TREE);
  }

  @After
  public void after() {
    conf.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    conf.setMetadataIndexType(metadataIndexType);
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  /** Example 1: 5 entities with 5 measurements each */
  @Test
  public void singleIndexTest1() {
    int deviceNum = 5;
    int measurementNum = 5;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + i;
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] =
            measurementPrefix + FileGenerator.generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 2: 1 entity with 150 measurements */
  @Test
  public void singleIndexTest2() {
    int deviceNum = 1;
    int measurementNum = 150;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + i;
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] =
            measurementPrefix + FileGenerator.generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 3: 150 entities with 1 measurement each */
  @Test
  public void singleIndexTest3() {
    int deviceNum = 150;
    int measurementNum = 1;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + FileGenerator.generateIndexString(i, deviceNum);
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] =
            measurementPrefix + FileGenerator.generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /** Example 4: 150 entities with 150 measurements each */
  @Test
  public void singleIndexTest4() {
    int deviceNum = 150;
    int measurementNum = 150;
    String[] devices = new String[deviceNum];
    int[][] vectorMeasurement = new int[deviceNum][];
    String[][] singleMeasurement = new String[deviceNum][];
    for (int i = 0; i < deviceNum; i++) {
      devices[i] = "d" + FileGenerator.generateIndexString(i, deviceNum);
      vectorMeasurement[i] = new int[0];
      singleMeasurement[i] = new String[measurementNum];
      for (int j = 0; j < measurementNum; j++) {
        singleMeasurement[i][j] =
            measurementPrefix + FileGenerator.generateIndexString(j, measurementNum);
      }
    }
    test(devices, vectorMeasurement, singleMeasurement);
  }

  /**
   * start test
   *
   * @param devices name and number of device
   * @param vectorMeasurement the number of device and the number of values to include in the tablet
   * @param singleMeasurement non-vector measurement name, set null if no need
   */
  private void test(String[] devices, int[][] vectorMeasurement, String[][] singleMeasurement) {
    // 1. generate file
    FileGenerator.generateFile(FILE_PATH, devices, vectorMeasurement, singleMeasurement);
    // 2. read metadata from file
    List<String> actualPaths = new ArrayList<>(); // contains all device by sequence
    readMetaDataDFS(actualPaths);

    List<String> actualDevices = new ArrayList<>(); // contains all device by sequence
    List<List<String>> actualMeasurements = new ArrayList<>(); // contains all device by sequence

    String lastDevice = null;
    for (String path : actualPaths) {
      String device = path.split(TsFileConstant.PATH_SEPARATER_NO_REGEX)[0];
      String measurement = path.split(TsFileConstant.PATH_SEPARATER_NO_REGEX)[1];
      if (!device.equals(lastDevice)) {
        actualDevices.add(device);
        List<String> measurements = new ArrayList<>();
        measurements.add(measurement);
        actualMeasurements.add(measurements);
      } else {
        actualMeasurements.get(actualMeasurements.size() - 1).add(measurement);
      }
      lastDevice = device;
    }

    // 3. generate correct result
    List<String> correctDevices = new ArrayList<>(); // contains all device by sequence
    List<List<String>> correctFirstMeasurements =
        new ArrayList<>(); // contains first measurements of every leaf, group by device
    generateCorrectResult(
        correctDevices, correctFirstMeasurements, devices, vectorMeasurement, singleMeasurement);
    // 4. compare correct result with TsFile's metadata
    Arrays.sort(devices);
    // 4.1 make sure device in order
    assertEquals(correctDevices.size(), devices.length);
    assertEquals(actualDevices.size(), correctDevices.size());
    for (int i = 0; i < actualDevices.size(); i++) {
      assertEquals(actualDevices.get(i), correctDevices.get(i));
    }
    // 4.2 make sure timeseries in order
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata();
      for (int j = 0; j < actualDevices.size(); j++) {
        for (int i = 0; i < actualMeasurements.get(j).size(); i++) {
          assertEquals(
              allTimeseriesMetadata.get(actualDevices.get(j)).get(i).getMeasurementId(),
              correctFirstMeasurements.get(j).get(i));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    // 4.3 make sure split leaf correctly
    for (int j = 0; j < actualDevices.size(); j++) {
      for (int i = 0; i < actualMeasurements.get(j).size(); i++) {
        assertEquals(
            actualMeasurements.get(j).get(i),
            correctFirstMeasurements.get(j).get(i * conf.getMaxDegreeOfIndexNode()));
      }
    }
  }

  /**
   * read TsFile metadata, load actual message in devices and measurements
   *
   * @param paths load actual paths
   */
  private void readMetaDataDFS(List<String> paths) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      TsFileMetadata tsFileMetaData = reader.readFileMetadata();
      BPlusTreeNode metadataIndexNode = (BPlusTreeNode) tsFileMetaData.getMetadataIndex();
      deviceDFS(paths, reader, metadataIndexNode);
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** DFS in device level load actual devices */
  private void deviceDFS(List<String> paths, TsFileSequenceReader reader, BPlusTreeNode node) {
    try {
      for (int i = 0; i < node.getChildren().size(); i++) {
        MetadataIndexEntry metadataIndexEntry = node.getChildren().get(i);
        long endOffset = node.getEndOffset();
        if (i != node.getChildren().size() - 1) {
          endOffset = node.getChildren().get(i + 1).getOffset();
        }
        BPlusTreeNode subNode =
            reader.getBPlusTreeIndexNode(metadataIndexEntry.getOffset(), endOffset);
        if (node.isLeaf()) {
          paths.add(metadataIndexEntry.getName());
        } else {
          deviceDFS(paths, reader, subNode);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * generate correct devices and measurements for test Note that if the metadata index tree is
   * re-designed, you may need to modify this function as well.
   *
   * @param correctDevices output
   * @param correctMeasurements output
   * @param devices input
   * @param vectorMeasurement input
   * @param singleMeasurement input
   */
  private void generateCorrectResult(
      List<String> correctDevices,
      List<List<String>> correctMeasurements,
      String[] devices,
      int[][] vectorMeasurement,
      String[][] singleMeasurement) {
    for (int i = 0; i < devices.length; i++) {
      String device = devices[i];
      correctDevices.add(device);
      // generate measurement and sort
      List<String> measurements = new ArrayList<>();
      // single-variable measurement
      if (singleMeasurement != null) {
        measurements.addAll(Arrays.asList(singleMeasurement[i]));
      }
      // multi-variable measurement
      for (int vectorIndex = 0; vectorIndex < vectorMeasurement[i].length; vectorIndex++) {
        String vectorName =
            vectorPrefix + FileGenerator.generateIndexString(vectorIndex, vectorMeasurement.length);
        measurements.add(vectorName);
        int measurementNum = vectorMeasurement[i][vectorIndex];
        for (int measurementIndex = 0; measurementIndex < measurementNum; measurementIndex++) {
          String measurementName =
              measurementPrefix
                  + FileGenerator.generateIndexString(measurementIndex, measurementNum);
          measurements.add(vectorName + TsFileConstant.PATH_SEPARATOR + measurementName);
        }
      }
      Collections.sort(measurements);
      correctMeasurements.add(measurements);
    }
    Collections.sort(correctDevices);
  }
}
