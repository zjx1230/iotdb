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
package org.apache.iotdb.db.index.indexrange;

public enum IndexRangeStrategyType {

  DEFAULT, NAIVE, UNIFORM;

  /**
   * judge the index type.
   *
   * @param i an integer used to determine index type
   * @return index type
   */
  public static IndexRangeStrategyType deserialize(short i) {
    switch (i) {
      case 1:
        return NAIVE;
      case 2:
        return UNIFORM;
      default:
        return DEFAULT;
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
      case NAIVE:
        return 1;
      case UNIFORM:
        return 2;
      default:
        return 0;
    }
  }


  public static IndexRangeStrategy getIndexStrategy(String indexTypeString) {
    String normalized = indexTypeString.toUpperCase();
    switch (normalized) {
      case "NAIVE":
        return new NaiveStrategy();
      case "UNIFORM":
        return new UniformStrategy();
      default:
        return new DefaultStrategy();
    }
  }
}
