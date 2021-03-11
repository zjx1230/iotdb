/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.index.common.IndexFunc.FeatureLayer.ALIGNED;
import static org.apache.iotdb.db.index.common.IndexFunc.FeatureLayer.IDENTIFIER;
import static org.apache.iotdb.db.index.common.IndexFunc.FeatureLayer.UNKNOWN_LAYER;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;

public enum IndexFunc {
  TIME_RANGE(INT64),
  SERIES_LEN(INT32),
  SIM_ST(INT64),
  SIM_ET(INT64),
  ED(DOUBLE),
  DTW(DOUBLE),
  CATEGORY,
  UNKNOWN;

  private static final Map<IndexFunc, FeatureLayer> funcLayers = new HashMap<>();

  static {
    funcLayers.put(SIM_ST, IDENTIFIER);
    funcLayers.put(SIM_ET, IDENTIFIER);
    funcLayers.put(TIME_RANGE, IDENTIFIER);
    funcLayers.put(SERIES_LEN, IDENTIFIER);

    funcLayers.put(ED, ALIGNED);
    funcLayers.put(DTW, ALIGNED);

    funcLayers.put(CATEGORY, UNKNOWN_LAYER);
    funcLayers.put(UNKNOWN, UNKNOWN_LAYER);
  }

  public static IndexFunc getIndexFunc(String indexFuncString) {
    String normalized = indexFuncString.toUpperCase();
    return IndexFunc.valueOf(normalized);
  }

  private TSDataType type;

  IndexFunc(TSDataType type) {
    this.type = type;
  }

  /**
   * For the index function with arbitrary or uncertain data type，it can always be converted into a
   * string by {@code Object.toString()}.
   */
  IndexFunc() {
    this.type = TSDataType.TEXT;
  }

  public static List<TSDataType> getSeriesByFunc(List<Path> paths, List<String> aggregations) {
    List<TSDataType> tsDataTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = getIndexFunc(aggregations.get(i)).getType();
      tsDataTypes.add(dataType);
    }
    return tsDataTypes;
  }

  public TSDataType getType() {
    return type;
  }

  public enum FeatureLayer {
    IDENTIFIER,
    ALIGNED,
    FEATURE,
    UNKNOWN_LAYER
  }

  public static FeatureLayer getFuncNeedFeatureLayer(IndexFunc func) {
    return funcLayers.getOrDefault(func, FeatureLayer.UNKNOWN_LAYER);
  }
}
