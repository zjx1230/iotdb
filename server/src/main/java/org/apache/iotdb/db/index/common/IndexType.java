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
package org.apache.iotdb.db.index.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexTypeException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.NoIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELBIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELBIndexNotGood;
import org.apache.iotdb.db.index.algorithm.paa.RTreePAAIndex;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

public enum IndexType {

  NO_INDEX,
  RTREE_PAA,
  ELB_INDEX,
  KV_INDEX,
  ELB;

  /**
   * judge the index type.
   *
   * @param i an integer used to determine index type
   * @return index type
   */
  public static IndexType deserialize(short i) {
    switch (i) {
      case 0:
        return NO_INDEX;
      case 1:
        return RTREE_PAA;
      case 2:
        return ELB_INDEX;
      case 3:
        return KV_INDEX;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  /**
   * judge the index deserialize type.
   *
   * @return the integer used to determine index type
   */
  public short serialize() {
    switch (this) {
      case NO_INDEX:
        return 0;
      case RTREE_PAA:
        return 1;
      case ELB_INDEX:
        return 2;
      case KV_INDEX:
        return 3;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static IndexType getIndexType(String indexTypeString)
      throws UnsupportedIndexTypeException {
    String normalized = indexTypeString.toUpperCase();
    try {
      return IndexType.valueOf(normalized);
    } catch (IllegalArgumentException e) {
      throw new UnsupportedIndexTypeException(indexTypeString);

    }
  }

  private static IoTDBIndex newIndexByType(String path, String indexDir, IndexType indexType,
      IndexInfo indexInfo) {
    switch (indexType) {
      case NO_INDEX:
        return new NoIndex(path, indexDir, indexInfo);
      case ELB_INDEX:
        return new ELBIndex(path, indexDir, indexInfo);
      case RTREE_PAA:
        return new RTreePAAIndex(path, indexDir, indexInfo);
      case ELB:
        return new ELBIndexNotGood(path, indexInfo);
      case KV_INDEX:
      default:
        throw new NotImplementedException("unsupported index type:" + indexType);
    }
  }

  public static IoTDBIndex constructIndex(String indexSeries, String indexDir, IndexType indexType,
      IndexInfo indexInfo) {
    indexInfo.setProps(uppercaseStringProps(indexInfo.getProps()));
    IoTDBIndex index = newIndexByType(indexSeries, indexDir, indexType, indexInfo);
    index.initPreprocessor(null, false);
    return index;
  }

  /**
   * Construct an index for an index-query
   */
  public static IoTDBIndex constructQueryIndex(String path, IndexType indexType,
      Map<String, Object> queryProps, List<IndexFuncResult> indexFuncs)
      throws UnsupportedIndexFuncException {
    queryProps = uppercaseProps(queryProps);
    IndexUtils.breakDown();
//    IndexInfo indexInfo = IndexManager.getInstance().getIndexRegister()
//        .getIndexInfoByPath(path, indexType);
    IndexInfo indexInfo = null;
    if (indexInfo == null) {
      throw new IllegalIndexParamException(
          String.format("%s.%s not found, why it escapes the check?", path, indexType));
    }
    IoTDBIndex index = newIndexByType(path, null, indexType, indexInfo);
    IndexUtils.breakDown("temp set index Dir is NULL!");

    index.initQuery(queryProps, indexFuncs);

    return index;
  }

  private static Map<String, String> uppercaseStringProps(Map<String, String> props) {
    Map<String, String> uppercase = new HashMap<>(props.size());
    props.forEach((k, v) -> uppercase.put(k.toUpperCase(), v.toUpperCase()));
    return uppercase;
  }

  private static Map<String, Object> uppercaseProps(Map<String, Object> props) {
    Map<String, Object> uppercase = new HashMap<>(props.size());
    for (Entry<String, Object> entry : props.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      if (v instanceof String) {
        uppercase.put(k.toUpperCase(), ((String) v).toUpperCase());
      } else {
        uppercase.put(k.toUpperCase(), v);
      }
    }
    return uppercase;
  }
}
