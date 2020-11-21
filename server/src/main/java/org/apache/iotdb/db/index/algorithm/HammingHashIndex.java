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
package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.index.common.IndexInfo;

/**
 * Hash lookup table is a data structure a structure that can map keys to values. A hash table uses
 * a hash function to compute an index, also called a hash code, into an array of buckets, from
 * which the desired value can be found. Generally, cost on retrieving a bucket can be regarded as
 * {@code O(1)}.<p>
 *
 * Hamming space retrieval returns data points within a Hamming ball of radius {@code H} for each
 * query. Therefore, it enables constant-time Approximate Nearest Neighbor (ANN) search through hash
 * lookups. <p>
 *
 * Based on the mature hash lookup algorithm and the Hamming space retrieval, a rich line of hash
 * methods focuses on better feature representations for preserving the similarity relationship in
 * the Hamming space.<p>
 *
 * TODO To be implemented.<p>
 */
public abstract class HammingHashIndex extends IoTDBIndex {

  public HammingHashIndex(String path, IndexInfo indexInfo) {
    super(path, null, indexInfo);
  }


}
