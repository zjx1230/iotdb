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
package org.apache.iotdb.db.index.it;

import ai.djl.Model;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.translate.Batchifier;
import ai.djl.translate.TranslateException;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** The file is for integration test for special TorchScript input. */
public class PytorchLoadTest {

  public static final Translator<NDArray, NDArray> translator =
      new Translator<NDArray, NDArray>() {
        @Override
        public NDList processInput(TranslatorContext ctx, NDArray input) {
          //      NDManager manager = ctx.getNDManager();
          //      NDArray array = manager.create(new float[]{input});
          //      return new NDList(array);
          return new NDList(input);
        }

        @Override
        public NDArray processOutput(TranslatorContext ctx, NDList list) {
          //      NDArray temp_arr = list.get(0);
          //      return temp_arr.getFloat();
          return list.get(0);
        }

        @Override
        public Batchifier getBatchifier() {
          // The Batchifier describes how to combine a batch together
          // Stacking, the most common batchifier, takes N [X1, X2, ...] arrays to a single [N, X1,
          // X2, ...] array
          return Batchifier.STACK;
        }
      };

  @Test
  public void testDictInput2() throws ModelException, IOException, TranslateException {
    Path modelDir =
        Paths.get("/Users/kangrong/code/github/deep-learning/hash_journal/TAH_project/src/");
    Model model = Model.newInstance("mmhh.pt");
    model.load(modelDir);
    System.out.println(model);
    Predictor<NDArray, NDArray> predictor = model.newPredictor(translator);
    NDManager manager = NDManager.newBaseManager();
    NDArray test = manager.ones(new Shape(1, 100));
    //    test.setName("input1.input");
    System.out.println(test);
    NDArray r = predictor.predict(test);
    for (int i = 0; i < 6; i++) {
      for (int j = 0; j < 8; j++) {
        System.out.print(String.format("%.4f, ", r.getFloat(i * 8 + j)));
      }
      System.out.println();
    }
  }
}
