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
package org.apache.iotdb.db.index.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The Stat module will provide researchers with a series of switchable lightweight tools and
 * functions for monitoring some measures commonly concerned in index research, such as disk access
 * count, cache hit ratio, index pruning ratio, index memory footprint and size, etc.
 *
 * <p>TODO To be completed
 */
public class IndexStatManager {

  private Map<Long, IndexStatStruct> stats = new HashMap<>();


  public static IndexStatManager getInstance() {
    return IndexStatManager.InstanceHolder.instance;
  }

  public void registerQuery(long queryId) {
    stats.put(queryId, new IndexStatStruct());
    startTotalQueryCost(queryId);
  }


  //  public void startFeatureExtractCost(long queryId) {
//    if (stats.containsKey(queryId)) {
//      IndexStatStruct stat = stats.get(queryId);
//      stat.nanoFeatureExtract = System.nanoTime();
//    }
//  }
//
//  public void stopFeatureExtractCost(long queryId) {
//    if (stats.containsKey(queryId)) {
//      IndexStatStruct stat = stats.get(queryId);
//      stat.featureExtractCost += System.nanoTime() - stat.nanoFeatureExtract;
//    }
//  }
  public void addFeatureExtractCost(long queryId, long st, long ed) {
    if (stats.containsKey(queryId)) {
      IndexStatStruct stat = stats.get(queryId);
      stat.featureExtractCost += ed - st;
    }
  }

  private void startTotalQueryCost(long queryId) {
    if (stats.containsKey(queryId)) {
      IndexStatStruct stat = stats.get(queryId);
      stat.nanoTotalQuery = System.nanoTime();
    }
  }

  private void stopTotalQueryCost(long queryId) {
    if (stats.containsKey(queryId)) {
      IndexStatStruct stat = stats.get(queryId);
      stat.totalQueryCost += System.nanoTime() - stat.nanoTotalQuery;
    }
  }

  public void startLoadRawDataCost(long queryId) {
    if (stats.containsKey(queryId)) {
      IndexStatStruct stat = stats.get(queryId);
      stat.nanoLoadRawData = System.nanoTime();
    }
  }

  public void stopLoadRawDataCost(long queryId) {
    if (stats.containsKey(queryId)) {
      IndexStatStruct stat = stats.get(queryId);
      stat.loadRawDataCost += System.nanoTime() - stat.nanoLoadRawData;
    }
  }

  private String provideReport(IndexStatStruct stat) {
    return String.format("total:%.3fms-feature:%.3fms-load:%.3fms", stat.totalQueryCost / 1000_000.,
        stat.featureExtractCost / 1000_000., stat.loadRawDataCost / 1000_000.);
  }

  public String deregisterQuery(long queryId) {
    stopTotalQueryCost(queryId);
    String ret = Optional.ofNullable(stats.get(queryId)).map(this::provideReport)
        .orElse("");
    stats.remove(queryId);
    return ret;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static IndexStatManager instance = new IndexStatManager();
  }

  private class IndexStatStruct {

    private long featureExtractCost = 0;
    private long totalQueryCost = 0;
    private long loadRawDataCost = 0;

    private long nanoFeatureExtract;
    private long nanoTotalQuery;
    private long nanoLoadRawData;

  }
}
