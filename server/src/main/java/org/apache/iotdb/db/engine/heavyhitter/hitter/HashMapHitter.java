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

package org.apache.iotdb.db.engine.heavyhitter.hitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashMapHitter implements QueryHeavyHitters {

  private static final Logger logger = LoggerFactory.getLogger(HashMapHitter.class);
  int hitter = IoTDBDescriptor.getInstance().getConfig().getMaxHitterNum();
  private Map<PartialPath, Integer> counter = new HashMap<>();
  private PriorityQueue<Entry<PartialPath, Integer>> topHeap = new PriorityQueue<>(hitter,
      new Comparator<Entry<PartialPath, Integer>>() {
        @Override
        public int compare(Entry<PartialPath, Integer> o1, Entry<PartialPath, Integer> o2) {
          return o2.getValue() - o1.getValue();
        }
      });

  public HashMapHitter(int maxHitterNum) {

  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    counter.put(queryPath, counter.getOrDefault(queryPath, 0) + 1);
  }

  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    List<PartialPath> ret = new ArrayList<>();
    topHeap.addAll(counter.entrySet());
    List<PartialPath> sgPaths = MManager.getInstance().getAllTimeseriesPath(sgName);
    for (int k = 0; k < hitter; k++) {
      if (!topHeap.isEmpty()) {
        PartialPath path =  topHeap.poll().getKey();
        if (sgPaths.contains(path)) {
          ret.add(path);
        }
      }
    }
    return ret;
  }

  /**
   * used to persist query frequency
   *
   * @param outputPath dump file name
   */
  public void printMapToFile(File outputPath) {
    try (BufferedWriter csvWriter = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8), 1024)) {
      File parent = outputPath.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      outputPath.createNewFile();
      // 写入文件内容
      for (Map.Entry<PartialPath, Integer> entry : counter.entrySet()) {
        String line = entry.getKey().getFullPath() + "," + entry.getValue();
        csvWriter.write(line);
        csvWriter.newLine();
      }
      csvWriter.flush();
    } catch (Exception e) {

    }
  }
}
