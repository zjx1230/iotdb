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

/**
 * The Stat module will provide researchers with a series of switchable lightweight tools and
 * functions for monitoring some measures commonly concerned in index research, such as disk access
 * count, cache hit ratio, index pruning ratio, index memory footprint and size, etc.
 *
 * <p>TODO To be completed
 */
public class IndexStatManager {

  public static long featureExtractCost = 0;
  public static long totalQueryCost = 0;
  public static long loadRawDataCost = 0;

  public static String provideReport() {
    // total:%.3fms-feature:%.3fms-load:%.3fms
    String ret =
        String.format(
            "&%.3f-%.3f-%.3f",
            totalQueryCost / 1000_000.,
            featureExtractCost / 1000_000.,
            loadRawDataCost / 1000_000.);
    featureExtractCost = 0;
    totalQueryCost = 0;
    loadRawDataCost = 0;
    return ret;
  }
}
