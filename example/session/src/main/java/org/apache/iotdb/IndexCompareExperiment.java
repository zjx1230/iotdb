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

import org.apache.iotdb.db.index.common.math.Randomwalk;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class IndexCompareExperiment {

  static SessionPool sessionPool = new SessionPool("127.0.0.1", 6667, "root", "root", 10);
  private static AtomicLong totalPoints;
  private static PrimitiveList list;

  private static final int DEVICE_NUM = 1;
//  private static final int DEVICE_NUM = 2;
  private static final int MEASUREMENT_NUM = 5;
//  private static final int MEASUREMENT_NUM = 50000;
  private static final int TOTAL_POINTS = 100;
//  private static final int TOTAL_POINTS = 100_000_000;

  public static void main(String[] args) throws InterruptedException {
    totalPoints = new AtomicLong();
    list = Randomwalk.generateRanWalk(TOTAL_POINTS);

    List<Thread> threadList = new ArrayList<>();

    for (int i = 0; i < DEVICE_NUM; i++) {
      for (int j = 0; j < MEASUREMENT_NUM; j++) {
        try {
          // create timeseries
          sessionPool.createTimeseries(
              "root.sg1.d" + i + ".s" + j,
              TSDataType.DOUBLE,
              TSEncoding.GORILLA,
              CompressionType.SNAPPY);

          // create index
          sessionPool.executeNonQueryStatement(
              "CREATE INDEX ON root.sg1.d" + i + ".s" + j + " WITH INDEX=ELB_INDEX, BLOCK_SIZE=5");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    long start = System.nanoTime();
    for (int i = 0; i < DEVICE_NUM; i++) {
      Thread thread = new Thread(new WriteThread(i));
      threadList.add(thread);
      thread.start();
    }

    for (Thread thread : threadList) {
      thread.join();
    }

    long time = (System.nanoTime() - start) / 1000_000_000;
    System.out.println("Average speed: " + totalPoints.get() / (time+1));
  }

  static class WriteThread implements Runnable {

    int device;

    WriteThread(int device) {
      this.device = device;
    }

    @Override
    public void run() {
      long time = 0;
      while (totalPoints.get() <= TOTAL_POINTS) {
        long start = System.nanoTime();

        time += 1;
        String deviceId = "root.sg1.d" + device;
        List<String> measurements = new ArrayList<>();
        for (int i = 0; i < MEASUREMENT_NUM; i++) {
          measurements.add("s" + i);
        }

        List<String> values = new ArrayList<>();
        for (int i = 0; i < MEASUREMENT_NUM; i++) {
          values.add(String.valueOf(list.getDouble((int) time)));
        }

        try {
          sessionPool.insertRecord(deviceId, time, measurements, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          e.printStackTrace();
        }
        totalPoints.getAndAdd(MEASUREMENT_NUM);
        float writeRate = (float) totalPoints.get() / TOTAL_POINTS;
        System.out.println(
            Thread.currentThread().getName()
                + " write "
                + MEASUREMENT_NUM
                + " cost: "
                + (System.nanoTime() - start) / 1000_000
                + "ms, " + String.format("finish %.1f%%", writeRate * 100));
      }
      try {
        sessionPool.executeNonQueryStatement(
            "flush");
        System.out.println("flushed");
      } catch (StatementExecutionException e) {
        e.printStackTrace();
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }
  }
}
