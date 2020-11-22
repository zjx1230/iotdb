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
package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.db.index.IndexTestUtils.TwoDimDoubleArrayToString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.db.index.algorithm.elb.ELBCountFixedFeatureExtractor;
import org.apache.iotdb.db.index.algorithm.elb.ELB.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.pattern.CalcParam;
import org.apache.iotdb.db.index.algorithm.elb.pattern.SingleSegmentationParam;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.index.distance.LNormDouble;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class ELBPreprocessorTest {

  @Test
  public void testCreateELBFeature() {
    String[] groundTruthL3 = new String[]{
        "{[-Inf,Inf],[321.60,-171.60],[396.60,-96.60],[471.60,-21.60],}",
        "{[-Inf,Inf],[366.60,-126.60],[441.60,-51.60],[516.60,23.40],}",
        "{[-Inf,Inf],[411.60,-81.60],[486.60,-6.60],[561.60,68.40],}",
        "{[-Inf,Inf],[456.60,-36.60],[531.60,38.40],[606.60,113.40],}",
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 30; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int slideStep = 3;
    int blockNum = 4;
    Distance distance = new LNormDouble(1);
    CalcParam calcParam = new SingleSegmentationParam(-1, 0.2, windowRange);
    ELBCountFixedFeatureExtractor elbProcessor = new ELBCountFixedFeatureExtractor(TSDataType.INT32,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.SEQ);
    elbProcessor.appendNewSrcData(srcData);
    assertL3(elbProcessor, groundTruthL3, true);
    elbProcessor.clear();
  }

  @Test
  public void testCreateELBFeatureElement() {
    String[] groundTruthL3 = new String[]{
        "{[23.40,-11.40],[38.40,3.60],[53.40,18.60],[68.40,33.60],}",
        "{[32.40,-2.40],[47.40,12.60],[62.40,27.60],[77.40,42.60],}",
        "{[41.40,6.60],[56.40,21.60],[71.40,36.60],[86.40,51.60],}",
        "{[50.40,15.60],[65.40,30.60],[80.40,45.60],[95.40,60.60],}",
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 30; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int slideStep = 3;
    int blockNum = 4;
    Distance distance = new LInfinityNormdouble();
    CalcParam calcParam = new SingleSegmentationParam(-1, 0.2, windowRange);
    ELBCountFixedFeatureExtractor elbProcessor = new ELBCountFixedFeatureExtractor(TSDataType.INT32,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.ELE, false, false, true, false);
    elbProcessor.appendNewSrcData(srcData);
    assertL3(elbProcessor, groundTruthL3, true);
    elbProcessor.clear();
    //Not store
    elbProcessor = new ELBCountFixedFeatureExtractor(TSDataType.INT32,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.ELE, false, false, false, false);
    elbProcessor.appendNewSrcData(srcData);
    assertL3(elbProcessor, groundTruthL3, false);
    elbProcessor.clear();
  }

  @Test
  public void testELBPrevious() throws IOException {
    String[] groundTruthL3 = new String[]{
        "{[59.40,24.60],[74.40,39.60],[89.40,54.60],[104.40,69.60],}",
        "{[68.40,33.60],[83.40,48.60],[98.40,63.60],[113.40,78.60],}",
        "{[77.40,42.60],[92.40,57.60],[107.40,72.60],[122.40,87.60],}",
        "{[86.40,51.60],[101.40,66.60],[116.40,81.60],[131.40,96.60],}",
        "{[95.40,60.60],[110.40,75.60],[125.40,90.60],[140.40,105.60],}",
        "{[104.40,69.60],[119.40,84.60],[134.40,99.60],[149.40,114.60],}",
        "{[113.40,78.60],[128.40,93.60],[143.40,108.60],[158.40,123.60],}",
        "{[122.40,87.60],[137.40,102.60],[152.40,117.60],[167.40,132.60],}",
        "{[131.40,96.60],[146.40,111.60],[161.40,126.60],[176.40,141.60],}",
        "{[140.40,105.60],[155.40,120.60],[170.40,135.60],[185.40,150.60],}",
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 30; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int slideStep = 3;
    int blockNum = 4;
    Distance distance = new LInfinityNormdouble();
    CalcParam calcParam = new SingleSegmentationParam(-1, 0.2, windowRange);
    ELBCountFixedFeatureExtractor elbProcessor = new ELBCountFixedFeatureExtractor(TSDataType.INT32,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.ELE, false, false, true, false);
    elbProcessor.appendNewSrcData(srcData);

    while (elbProcessor.hasNext()) {
      elbProcessor.processNext();
    }
    ByteBuffer previous = elbProcessor.serializePrevious();
    elbProcessor.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 30; i < 60; i++) {
      srcData2.putInt(i * 3, i * 3);
    }
    boolean storeFeature = true;
    ELBCountFixedFeatureExtractor elbProcessor2 = new ELBCountFixedFeatureExtractor(TSDataType.INT32,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.ELE, false, false,
        storeFeature, false);

    elbProcessor2.deserializePrevious(previous);
    elbProcessor2.appendNewSrcData(srcData2);
    assertL3(elbProcessor2, groundTruthL3, storeFeature);
    elbProcessor2.closeAndRelease();
  }

  private void assertL3(ELBCountFixedFeatureExtractor elbProcessor, String[] groundTruthL3,
      boolean storeL3) {
    int idx = 0;
    while (elbProcessor.hasNext()) {
//      System.out.println("idx:" + idx);
      elbProcessor.processNext();
      //L3 latest
      double[][] elbL3 = (double[][]) elbProcessor.getCurrent_L3_Feature();
//      System.out.println(TwoDimDoubleArrayToString(elbL3));
      Assert.assertEquals(groundTruthL3[idx], TwoDimDoubleArrayToString(elbL3));
      //L2 latest N
      List<Object> L3s = elbProcessor.getLatestN_L3_Features(idx + 5);
      if (storeL3) {
        Assert.assertEquals(idx + 1, L3s.size());
        for (int i = 0; i <= idx; i++) {
          double[][] elb = (double[][]) L3s.get(i);
//          System.out.println(TwoDimDoubleArrayToString(elb));
          Assert.assertEquals(groundTruthL3[i], TwoDimDoubleArrayToString(elb));
        }
      } else {
        //if not storeL3, it will only return the latest one
        Assert.assertEquals(1, L3s.size());
        double[][] elb = (double[][]) L3s.get(0);
//        System.out.println(TwoDimDoubleArrayToString(elb));
        Assert.assertEquals(groundTruthL3[idx], TwoDimDoubleArrayToString(elb));
      }
      idx++;
    }
  }
}
