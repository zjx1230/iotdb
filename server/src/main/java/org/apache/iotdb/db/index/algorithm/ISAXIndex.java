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
 * ISAXIndex first splits the input data into segments and extracts their discrete SAX features, and
 * then inserts the data features into a root leaf node.  When the number of series of a leaf node
 * reaches the given threshold, ISAXIndex will upscale the resolution of SAX features, thus dividing
 * them into two leaf nodes. The SAX lower bound constraint is used to guarantee no-false-dismissals
 * pruning.<p>
 *
 * In some indexing techniques, ISAXIndex is composed of a list of subtrees.Suppose the input series
 * is divided into {@code b} segments, all data will be first divided into {@code 2^b} sub-index
 * tree according to the first-bits of SAX features.<p>
 *
 * TODO To be implemented.<p>
 */
public abstract class ISAXIndex extends IoTDBIndex {

  public ISAXIndex(String path, IndexInfo indexInfo) {
    super(path, null, indexInfo);
  }
}
