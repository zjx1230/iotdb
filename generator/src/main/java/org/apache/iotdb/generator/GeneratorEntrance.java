/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.generator;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.ClusterSession;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorEntrance {
  private static final Logger logger = LoggerFactory.getLogger(GeneratorEntrance.class);

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    String[] addressElements = args[0].split(":");
    String seriesPath = args[1];
    int timeInterval = Integer.parseInt(args[2]) * 1000;
    int batchNum = Integer.parseInt(args[3]);
    String[] pathElements = seriesPath.split("\\.");
    String measurementId = pathElements[pathElements.length - 1];
    String deviceId = seriesPath.substring(0, seriesPath.length() - measurementId.length() - 1);

    ClusterSession clusterSession =
        new ClusterSession(addressElements[0], Integer.parseInt(addressElements[1]));

    long timestampForInsert = 0;
    while (true) {
      long startTime = System.currentTimeMillis();
      Tablet tablet =
          Generator.generateTablet(
              deviceId, pathElements[pathElements.length - 1], timestampForInsert, batchNum);
      clusterSession.insertTablet(tablet);
      logger.info("Insert {} data points to {}", batchNum, seriesPath);
      String query = String.format("select count(%s) from %s", measurementId, deviceId);
      SessionDataSet sessionDataSet = clusterSession.queryTablet(query, deviceId);
      logger.info(
          "Execute query {} with result : {}", query, sessionDataSet.next().getFields().get(0));
      timestampForInsert += batchNum;
      long endTime = System.currentTimeMillis();
      if (timeInterval - (endTime - startTime) > 0) {
        Thread.sleep(timeInterval - (endTime - startTime));
      }
    }
  }
}
