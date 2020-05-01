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

import java.nio.ByteBuffer;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.NoIndex;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public enum IndexType {

  NO_INDEX, PAA, ELB, KV_INDEX;

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
    switch (indexType) {
      case PAA:
      case ELB:
      case KV_INDEX:
        throw new NotImplementedException("unsupported index type:" + indexType);
      case NO_INDEX:
      default:
        return new NoIndex(path, indexInfo);
    }
  }
}
