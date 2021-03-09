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
package org.apache.iotdb.db.index.algorithm.mmhh;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.Batchifier;
import ai.djl.translate.TranslateException;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.feature.WholeMatchFeatureExtractor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For PAA feature in the whole matching. A simplified version all PAA PAA (Piecewise Aggregate
 * Approximation), a classical feature in time series.
 *
 * <p>Refer to: Rong Kang, et al. "Dimensionality reduction for fast similarity search in large
 * time series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class MMHHWholeFeatureExtractor extends WholeMatchFeatureExtractor {

  private static final Logger logger = LoggerFactory.getLogger(MMHHWholeFeatureExtractor.class);

  private static final String NON_SUPPORT_MSG = "For whole matching, it's not supported";
  private final int alignedLength;
  private final int hashBitLength;
  private long binaryBitIn64;
  private final float[] inputArray;
  private NDManager manager;

  private final Translator<NDArray, NDArray> translator = new Translator<NDArray, NDArray>() {
    @Override
    public NDList processInput(TranslatorContext ctx, NDArray input) {
      return new NDList(input);
    }

    @Override
    public NDArray processOutput(TranslatorContext ctx, NDList list) {
      return list.get(0);
    }

    @Override
    public Batchifier getBatchifier() {
      return Batchifier.STACK;
    }
  };
  private Predictor<NDArray, NDArray> predictor;
  private Model model;

  public MMHHWholeFeatureExtractor(
      String modelPath, int alignedLength, int hashBitLength)
      throws IOException, MalformedModelException {
    super(true);
    assert hashBitLength < 64;
    this.alignedLength = alignedLength;
    this.hashBitLength = hashBitLength;
    this.binaryBitIn64 = 0;
    this.inputArray = new float[alignedLength];
    loadMMHHModel(modelPath);
  }

  private void loadMMHHModel(String modelPath) throws IOException, MalformedModelException {
    if (!(new File(modelPath).exists())) {
      throw new IllegalIndexParamException("given path is not exist: " + modelPath);
    }
    String[] paths = modelPath.split(File.separator);
    String modelFilePath = paths[paths.length - 1];
    String modelDirPath = modelPath.substring(0, modelPath.length() - modelFilePath.length());
    Path modelDir = Paths.get(modelDirPath);
    model = Model.newInstance(modelFilePath);
    model.load(modelDir);
    this.predictor = model.newPredictor(translator);
    this.manager = NDManager.newBaseManager();
  }

  public void close() {
    this.predictor.close();
    this.model.close();
  }

  @Override
  public Long getCurrent_L3_Feature() {
    return binaryBitIn64;
  }

  /**
   * For Whole mathing, it's will deep copy
   */
  @Override
  public TVList getCurrent_L2_AlignedSequence() {
    TVList res = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
    double timeInterval =
        ((double) (srcData.getLastTime() - srcData.getMinTime())) / (srcData.size() - 1);
    for (int i = 0; i < alignedLength; i++) {
      int idx = i >= srcData.size() ? srcData.size() - 1 : i;
      long t = (long) (srcData.getMinTime() + timeInterval * i);
      res.putDouble(t, IndexUtils.getDoubleFromAnyType(srcData, idx));
    }
    return res;

  }

  /**
   * 两件事：长度为aligned_len，不足补齐，多了不管
   */
  private void fillHashBitArray() {
    //    featureArray
    for (int i = 0; i < inputArray.length; i++) {
      if (i >= srcData.size()) {
        inputArray[i] = 0;
        continue;
      }
      switch (srcData.getDataType()) {
        case INT32:
          inputArray[i] = srcData.getInt(i);
          break;
        case INT64:
          inputArray[i] = srcData.getLong(i);
          break;
        case FLOAT:
          inputArray[i] = srcData.getFloat(i);
          break;
        case DOUBLE:
          inputArray[i] = (float) srcData.getDouble(i);
          break;
        default:
          throw new NotImplementedException(srcData.getDataType().toString());
      }
    }
    try (NDArray input = manager.create(inputArray)) {
      NDArray hashBitND = predictor.predict(input);
      binaryBitIn64 = 0;
      // fill HashBits to floatArray
      for (int i = 0; i < hashBitLength; i++) {
        binaryBitIn64 |= (((hashBitND.getFloat(i) > 0) ? 1L : 0L) << i);
      }
    } catch (TranslateException e) {
      logger.error("meet errors when predict MMHH", e);
    }
  }

  public Long processQuery(double[] patterns) {
    //    featureArray
    for (int i = 0; i < inputArray.length; i++) {
      if (i >= patterns.length) {
        inputArray[i] = 0;
        continue;
      }
      inputArray[i] = (float) patterns[i];
    }
    try (NDArray input = manager.create(inputArray)) {
      NDArray hashBitND = predictor.predict(input);
      binaryBitIn64 = 0;
      // fill HashBits to floatArray
      for (int i = 0; i < hashBitLength; i++) {
        binaryBitIn64 |= (((hashBitND.getFloat(i) > 0) ? 1L : 0L) << i);
      }
    } catch (TranslateException e) {
      logger.error("meet errors when predict MMHH", e);
    }
    return binaryBitIn64;
  }

  @Override
  public void processNext() {
    fillHashBitArray();
    hasNewData = false;
  }

  @Override
  public void clearProcessedSrcData() {
    this.srcData = null;
  }
}
