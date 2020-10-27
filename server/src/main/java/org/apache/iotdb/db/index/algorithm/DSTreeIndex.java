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
 * <p>The DSTree approach uses the EAPCA representation technique, which allows, during node
 * splitting, the resolution of a representation to increase along two dimensions: vertically and
 * horizontally. (Instead, SAX-based indexes allow horizontal splitting by adding a breakpoint to
 * the y-axis, and SFA allows vertical splitting by adding a new DFT coefficient.) In addition to a
 * lower bounding distance, the DSTree also supports an upper bounding distance. It uses both
 * distances to determine the optimal splitting policy for each node.
 * </p>
 *
 * <p>The Lernaean Hydra of Data Series Similarity Search: An Experimental Evaluation of the State
 * of the Art Echihabi et al. VLDB2018</p>
 *
 * TODO To be implemented.<p>
 */
public abstract class DSTreeIndex extends IoTDBIndex {

  public DSTreeIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo);
  }
}
