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

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.File;
import java.util.Random;

/**
 * An example of writing data with TSRecord to TsFile It uses the interface: public void
 * addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
 */
public class TsFileWriteTest {
  public static int deviceNum;
  public static int sensorNum;
  public static int fileNum;
  public static int pointNum = 10;

  public static void main(String[] args) {
    try {
      deviceNum = 10000; // Integer.parseInt(cl.getOptionValue("d"));
      sensorNum = 100; // Integer.parseInt(cl.getOptionValue("m"));
      fileNum = 1; // Integer.parseInt(cl.getOptionValue("f"));
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      try {
        String path =
            "/home/fit/xkf"
                + "/without/"
                + deviceNum
                + "."
                + sensorNum
                + "/test"
                + fileIndex
                + ".tsfile";
        File f = FSFactoryProducer.getFSFactory().getFile(path);
        if (f.exists()) {
          f.delete();
        }

        try {
          TsFileWriter tsFileWriter = new TsFileWriter(f);
          for (int i = 1; i <= deviceNum; i++) {
            for (int j = 1; j <= sensorNum; j++) {
              Path path1 = new Path(Constant.DEVICE_PREFIX + i);
              tsFileWriter.registerTimeseries(
                  path1,
                  new UnaryMeasurementSchema(
                      Constant.SENSOR_ + j,
                      TSDataType.INT64,
                      TSEncoding.RLE,
                      CompressionType.GZIP));
            }
          }

          long curTime = System.nanoTime();
          // construct TSRecord
          int count = 0;
          for (int j = 1; j <= deviceNum; j++) {
            for (int i = 1; i <= pointNum; i++) {
              TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_PREFIX + j);
              for (int t = 1; t <= sensorNum; t++) {
                DataPoint dPoint1 =
                    new LongDataPoint(Constant.SENSOR_ + t, new Random().nextLong());
                tsRecord.addTuple(dPoint1);
                count++;
              }
              // write TSRecord
              tsFileWriter.write(tsRecord);
              if (count % 100000 == 0) {
                System.out.println(count);
                tsFileWriter.flushAllChunkGroups();
              }
            }
          }
          tsFileWriter.close();
          long finishTime = System.nanoTime();

          System.out.println(
              "write finished time: " + (finishTime - curTime) / 1000 / 1000 + " ms.");
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println(e.getMessage());
        }
      } catch (Throwable e) {
        e.printStackTrace();
        System.out.println(e.getMessage());
      }
    }
  }
}
