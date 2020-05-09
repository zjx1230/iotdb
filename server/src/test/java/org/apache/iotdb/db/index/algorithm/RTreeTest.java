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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.algorithm.RTree.SeedPicker;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

public class RTreeTest {

  @Test
  public void testRTreeToString() {
    String gt = "maxEntries:4,minEntries:2,numDims:2,seedPicker:LINEAR\n"
        + "Node{coords=[0.0, 0.0], dimensions=[19.0, 17.0], leaf=false}\n"
        + "--Node{coords=[0.0, 0.0], dimensions=[19.0, 17.0], leaf=false}\n"
        + "----Node{coords=[15.0, 13.0], dimensions=[4.0, 1.0], leaf=true}\n"
        + "------Entry: 4,Node{coords=[19.0, 14.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 2,Node{coords=[15.0, 13.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "----Node{coords=[2.0, 2.0], dimensions=[15.0, 15.0], leaf=true}\n"
        + "------Entry: 5,Node{coords=[17.0, 17.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 15,Node{coords=[5.0, 5.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 17,Node{coords=[2.0, 2.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 18,Node{coords=[5.0, 13.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "----Node{coords=[0.0, 0.0], dimensions=[12.0, 8.0], leaf=true}\n"
        + "------Entry: 3,Node{coords=[11.0, 1.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 0,Node{coords=[0.0, 8.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 1,Node{coords=[9.0, 7.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 12,Node{coords=[12.0, 0.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "----Node{coords=[3.0, 2.0], dimensions=[10.0, 1.0], leaf=true}\n"
        + "------Entry: 6,Node{coords=[13.0, 2.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 13,Node{coords=[3.0, 2.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 14,Node{coords=[12.0, 3.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "--Node{coords=[1.0, 0.0], dimensions=[17.0, 15.0], leaf=false}\n"
        + "----Node{coords=[4.0, 7.0], dimensions=[0.0, 8.0], leaf=true}\n"
        + "------Entry: 8,Node{coords=[4.0, 15.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 11,Node{coords=[4.0, 7.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "----Node{coords=[17.0, 7.0], dimensions=[1.0, 8.0], leaf=true}\n"
        + "------Entry: 19,Node{coords=[18.0, 15.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 16,Node{coords=[17.0, 7.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "----Node{coords=[1.0, 0.0], dimensions=[14.0, 8.0], leaf=true}\n"
        + "------Entry: 9,Node{coords=[1.0, 0.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 7,Node{coords=[15.0, 4.0], dimensions=[0.0, 0.0], leaf=true}\n"
        + "------Entry: 10,Node{coords=[3.0, 8.0], dimensions=[0.0, 0.0], leaf=true}\n";
    int dim = 2;
    Random random = new Random(0);
    RTree<Integer> rTree = new RTree<>(4, 2, 2, SeedPicker.LINEAR);
    for (int i = 0; i < 20; i++) {
      float[] in = new float[2];
      for (int j = 0; j < dim; j++) {
        in[j] = ((float) random.nextInt(20));
      }
      System.out.println(String.format("add: %s, value: %d", Arrays.toString(in), i));
      rTree.insert(in, i);
    }
//    System.out.println(inputData);
    System.out.println(rTree);
    Assert.assertEquals(gt, rTree.toString());
  }

  @Test
  public void testRTreeSerialization() throws IOException {
    int dim = 2;
    Random random = new Random(0);
    RTree<Integer> rTree = new RTree<>(4, 2, 2, SeedPicker.LINEAR);
    int dataSize = 20;
    int[] beforeData = new int[dataSize];
    for (int i = 0; i < 20; i++) {
      float[] in = new float[2];
      for (int j = 0; j < dim; j++) {
        in[j] = ((float) random.nextInt(20));
      }
      System.out.println(String.format("add: %s, value: %d", Arrays.toString(in), i));
      rTree.insert(in, i);
      beforeData[i] = i;
    }

    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(dataSize, boas);
    BiConsumer<Integer, OutputStream> serial = (i, o) -> {
      try {
        ReadWriteIOUtils.write(beforeData[i], o);
      } catch (IOException e) {
        e.printStackTrace();
      }
    };
    rTree.serialize(boas, serial);
    //deserialize
    ByteBuffer byteBuffer = ByteBuffer.wrap(boas.toByteArray());
    int afterSize = ReadWriteIOUtils.readInt(byteBuffer);
    int[] afterData = new int[afterSize];
    BiConsumer<Integer, ByteBuffer> deserial = (i, b) -> {
      int value = ReadWriteIOUtils.readInt(b);
      afterData[i] = value;
    };
    RTree<Integer> afterRTree = RTree.deserialize(byteBuffer, deserial);
    System.out.println(rTree);
    System.out.println(afterRTree);
    Assert.assertEquals(rTree.toString(), afterRTree.toString());
    Assert.assertArrayEquals(beforeData, afterData);

  }

}