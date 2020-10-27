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
package org.apache.iotdb.db.index.algorithm.paa;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.preprocess.TimeFixedFeatureExtractor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * PAA (Piecewise Aggregate Approximation), a classical feature in time series. <p>
 *
 * Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large time
 * series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAAFeatureExtractor extends IndexFeatureExtractor {

  public PAAFeatureExtractor(TSDataType dataType,
      WindowType widthType, int windowRange, int slideStep) {
    super(dataType, widthType, windowRange, slideStep);
  }

  @Override
  protected void initParams() {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public boolean hasNext(Filter timeFilter) {
    return false;
  }

  @Override
  public void processNext() {

  }

  @Override
  public int getCurrentChunkOffset() {
    return 0;
  }

  @Override
  public int getCurrentChunkSize() {
    return 0;
  }

  @Override
  public List<Identifier> getLatestN_L1_Identifiers(int latestN) {
    return null;
  }

  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    return null;
  }

  @Override
  public long getChunkStartTime() {
    return 0;
  }

  @Override
  public long getChunkEndTime() {
    return 0;
  }

  @Override
  public long clear() {
    return 0;
  }

  @Override
  public int getAmortizedSize() {
    return 0;
  }

  @Override
  public int nextUnprocessedWindowStartIdx() {
    return 0;
  }
}
