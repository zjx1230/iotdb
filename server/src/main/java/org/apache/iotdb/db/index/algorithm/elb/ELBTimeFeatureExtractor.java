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
package org.apache.iotdb.db.index.algorithm.elb;

import org.apache.iotdb.db.index.feature.TimeFixedFeatureExtractor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * A preprocessor for ELB Matching which calculates the mean value of a list of adjacent blocks over
 * stream/long series.
 *
 * <p>ELB-Match is a scan-based method for accelerating the subsequence similarity query. It stores
 * the block features during the index building phase.
 *
 * <p>Refer to: Kang, Rong, et al. Matching Consecutive Subpatterns over Streaming Time Series.
 * APWeb-WAIM. Springer. 2018: 90-105.
 *
 * <p>Temp
 */
public class ELBTimeFeatureExtractor extends TimeFixedFeatureExtractor {

  public ELBTimeFeatureExtractor(
      TSDataType tsDataType,
      int windowRange,
      int slideStep,
      int alignedDim,
      long timeAnchor,
      boolean storeIdentifier,
      boolean storeAligned,
      boolean inQueryMode) {
    super(
        tsDataType,
        windowRange,
        slideStep,
        alignedDim,
        timeAnchor,
        storeIdentifier,
        storeAligned,
        inQueryMode);
  }

  public ELBTimeFeatureExtractor(
      TSDataType tsDataType,
      int windowRange,
      int slideStep,
      int alignedDim,
      long timeAnchor,
      boolean storeIdentifier,
      boolean storeAligned) {
    super(
        tsDataType, windowRange, slideStep, alignedDim, timeAnchor, storeIdentifier, storeAligned);
  }

  public ELBTimeFeatureExtractor(
      TSDataType tsDataType, int windowRange, int alignedDim, int slideStep, long timeAnchor) {
    super(tsDataType, windowRange, alignedDim, slideStep, timeAnchor);
  }
}
