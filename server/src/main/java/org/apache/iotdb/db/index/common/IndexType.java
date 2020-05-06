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

import static org.apache.iotdb.db.index.common.IndexFunc.SIM_ET;
import static org.apache.iotdb.db.index.common.IndexFunc.LEN;
import static org.apache.iotdb.db.index.common.IndexFunc.SIM_ST;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.NoIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELBIndex;
import org.apache.iotdb.db.index.algorithm.paa.PAAIndex;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

public enum IndexType {

  NO_INDEX(SIM_ST, SIM_ET, LEN),
  PAA(SIM_ST, SIM_ET, LEN),
  ELB(SIM_ST, SIM_ET, LEN),
  KV_INDEX();

  private final Set<IndexFunc> func;

  IndexType(IndexFunc... func) {
    this.func = new HashSet<>(Arrays.asList(func));
  }

  public Set<IndexFunc> getSupportedFunc() {
    return func;
  }

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
        return PAA;
      case 2:
        return ELB;
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
      case PAA:
        return 1;
      case ELB:
        return 2;
      case KV_INDEX:
        return 3;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static IndexType getIndexType(String indexTypeString) throws IndexManagerException {
    String normalized = indexTypeString.toUpperCase();
    switch (normalized) {
      case "NO_INDEX":
        return NO_INDEX;
      case "PAA":
        return PAA;
      case "ELB":
        return ELB;
      case "KV_INDEX":
        return KV_INDEX;
      default:
        throw new IndexManagerException("unsupported index type:" + indexTypeString);
    }
  }

  public static IoTDBIndex constructIndex(String path, IndexType indexType, IndexInfo indexInfo) {
    uppercaseProps(indexInfo);
    switch (indexType) {
      case ELB:
        return new ELBIndex(path, indexInfo);
      case PAA:
        return new PAAIndex(path, indexInfo);
      case NO_INDEX:
        return new NoIndex(path, indexInfo);
      case KV_INDEX:
      default:
        throw new NotImplementedException("unsupported index type:" + indexType);
    }
  }

  private static void uppercaseProps(IndexInfo indexInfo) {
    Map<String, String> props = indexInfo.getProps();
    Map<String, String> uppercase = new HashMap<>(props.size());
    props.forEach((k, v) -> uppercase.put(k.toUpperCase(), v.toUpperCase()));
    indexInfo.setProps(uppercase);
  }
}
