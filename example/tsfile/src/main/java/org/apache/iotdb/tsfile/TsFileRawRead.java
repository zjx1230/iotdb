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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;

import java.io.IOException;

public class TsFileRawRead {

  private static final String DEVICE1 = "device_1";
  public static int deviceNum;
  public static int sensorNum;
  public static int treeType; // 0=Zesong Tree, 1=B+ Tree
  public static int fileNum;

  public static void main(String[] args) throws IOException {
    try {
      deviceNum = 10000; // Integer.parseInt(cl.getOptionValue("d"));
      sensorNum = 100; // Integer.parseInt(cl.getOptionValue("m"));
      fileNum = 1; // Integer.parseInt(cl.getOptionValue("f"));
    } catch (Exception e) {
      e.printStackTrace();
    }

    long totalStartTime = System.nanoTime();
    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      // file path
      String path =
          "/Users/SilverNarcissus/iotdb/tsfile_test"
              + "/withoutStatAndMeasurement_1/"
              + deviceNum
              + "."
              + sensorNum
              + "/test"
              + fileIndex
              + ".tsfile";

      // raw data query
      try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
          TsFileReader readTsFile = new TsFileReader(reader)) {
        reader.readTimeseriesMetadata(new Path("device_1", "sensor_1"), true);
      }
    }
    long totalTime = (System.nanoTime() - totalStartTime) / 1000_000;
    System.out.println("Total raw read cost time: " + totalTime + "ms");
    System.out.println("Average cost time: " + (double) totalTime / (double) fileNum + "ms");
  }
}
