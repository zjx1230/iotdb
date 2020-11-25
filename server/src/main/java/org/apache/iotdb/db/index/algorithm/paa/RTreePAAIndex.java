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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.iotdb.db.index.algorithm.RTreeIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.TriFunction;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Use PAA as the feature of MBRIndex.</p>
 */

public class RTreePAAIndex extends RTreeIndex {

  private static final Logger logger = LoggerFactory.getLogger(RTreePAAIndex.class);
  private final int paaWidth;

  private PAAWholeFeatureExtractor paaWholeFeatureExtractor;

  // Only for query
  private Map<Integer, Identifier> identifierMap = new HashMap<>();

  public RTreePAAIndex(PartialPath path,
      TSDataType tsDataType, String indexDir,
      IndexInfo indexInfo) {
    super(path, tsDataType, indexDir, indexInfo, true);
    paaWidth = seriesLength / featureDim;

  }

  @Override
  public void initPreprocessor(ByteBuffer previous, boolean inQueryMode) {
    if (this.indexFeatureExtractor != null) {
      this.indexFeatureExtractor.clear();
    }
    this.paaWholeFeatureExtractor = new PAAWholeFeatureExtractor(tsDataType, seriesLength,
        featureDim, false, currentLowerBounds);
    paaWholeFeatureExtractor.deserializePrevious(previous);
    this.indexFeatureExtractor = paaWholeFeatureExtractor;
  }

  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  @Override
  protected void fillCurrentFeature() {
    // do nothing.
    // we have pass the {@code currentLowerBounds} to PAA extractor.
    // PAA extractor will directly calculate and update the passed-in currentCorners.
  }

//  @Override
//  protected BiConsumer<Integer, OutputStream> getSerializeFunc() {
//    return (idx, outputStream) -> {
//      try {
//        paaWholeFeatureExtractor.serializeIdentifier(idx, outputStream);
//      } catch (IOException e) {
//        logger.error("serialized error.", e);
//      }
//    };
//  }
//
//
//  @Override
//  protected BiConsumer<Integer, InputStream> getDeserializeFunc() {
//    return (idx, input) -> {
//      Identifier identifier = null;
//      try {
//        identifier = Identifier.deserialize(input);
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//      identifierMap.put(idx, identifier);
//    };
//  }

//  @Override
//  protected List<Identifier> getQueryCandidates(List<Integer> candidateIds) {
//    List<Identifier> res = new ArrayList<>(candidateIds.size());
//    candidateIds.forEach(i -> res.add(identifierMap.get(i)));
//    this.identifierMap.clear();
//    return res;
//  }


  /**
   * PAA has lower bounding property.
   */
  @Override
  protected double calcLowerBoundThreshold(double queryThreshold) {
    return queryThreshold;
  }

  @Override
  protected float[] calcQueryFeature(double[] patterns) {
    float[] res = new float[featureDim];
    for (int i = 0; i < featureDim; i++) {
      for (int j = 0; j < paaWidth; j++) {
        res[i] += patterns[i * paaWidth + j];
      }
      res[i] /= paaWidth;
    }
    return res;
  }

  @Override
  protected TriFunction<float[], float[], float[], Double> getCalcLowerDistFunc() {
    return (lowerBounds, upperBounds, queryFeatures) -> {
      double sum = 0;
      for (int i = 0; i < queryFeatures.length; i++) {
        double dp;
        if (queryFeatures[i] < lowerBounds[i]) {
          dp = lowerBounds[i] - queryFeatures[i];
        } else if (queryFeatures[i] > upperBounds[i]) {
          dp = queryFeatures[i] - upperBounds[i];
        } else {
          dp = 0;
        }
        sum += paaWidth * dp * dp;
      }
      return Math.sqrt(sum);
    };
  }

  @Override
  protected BiFunction<double[], TVList, Double> getCalcExactDistFunc() {
    return (queryTs, tvList) -> {
      double sum = 0;
      for (int i = 0; i < queryTs.length; i++) {
        final double dp = queryTs[i] - IndexUtils.getDoubleFromAnyType(tvList, i);
        sum += dp * dp;
      }
      return Math.sqrt(sum);
    };
  }


  protected IndexFeatureExtractor createQueryFeatureExtractor() {
    return new PAAWholeFeatureExtractor(tsDataType, seriesLength,
        featureDim, true, currentLowerBounds);
  }

}
