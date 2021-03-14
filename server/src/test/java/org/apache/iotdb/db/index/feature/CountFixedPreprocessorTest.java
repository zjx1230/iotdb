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
package org.apache.iotdb.db.index.feature;

import org.apache.iotdb.db.index.IndexTestUtils;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CountFixedPreprocessorTest {

  @Test
  public void testCreateAlignedSequence() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[0-4,5]", "[2-6,5]", "[4-8,5]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[0,0],[1,1],[2,2],[3,3],[4,4],}",
          "{[2,2],[3,3],[4,4],[5,5],[6,6],}",
          "{[4,4],[5,5],[6,6],[7,7],[8,8],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 9; i++) {
      srcData.putInt(i, i);
    }
    int windowRange = 5;
    int slideStep = 2;
    CountFixedFeatureExtractor countFixed =
        new CountFixedFeatureExtractor(TSDataType.INT32, windowRange, slideStep, true, true);
    countFixed.appendNewSrcData(srcData);
    assertL1AndL2(countFixed, groundTruthL1, groundTruthL2);
    countFixed.closeAndRelease();

    System.out.println();
    //    CountFixedPreprocessor countFixedWithoutStored = new CountFixedPreprocessor(srcData,
    //        windowRange, slideStep, false, false);
    //    assertL1AndL2(countFixedWithoutStored, groundTruthL1, groundTruthL2);
    //    countFixedWithoutStored.clear();
  }

  @Test
  public void testCreateAlignedSequence2() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[0-12,5]", "[6-18,5]", "[12-24,5]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[0,0],[3,3],[6,6],[9,9],[12,12],}",
          "{[6,6],[9,9],[12,12],[15,15],[18,18],}",
          "{[12,12],[15,15],[18,18],[21,21],[24,24],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 9; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 5;
    int slideStep = 2;
    CountFixedFeatureExtractor countFixed =
        new CountFixedFeatureExtractor(TSDataType.INT32, windowRange, slideStep, true, true);
    countFixed.appendNewSrcData(srcData);
    assertL1AndL2(countFixed, groundTruthL1, groundTruthL2);
    countFixed.closeAndRelease();

    System.out.println();
    //    CountFixedPreprocessor countFixedWithoutStored = new CountFixedPreprocessor(srcData,
    //        windowRange, slideStep, false, false);
    //    assertL1AndL2(countFixedWithoutStored, groundTruthL1, groundTruthL2);
    //    countFixedWithoutStored.clear();
  }

  @Test
  public void testPrevious() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[6-10,5]", "[8-12,5]", "[10-14,5]", "[12-16,5]", "[14-18,5]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[6,6],[7,7],[8,8],[9,9],[10,10],}",
          "{[8,8],[9,9],[10,10],[11,11],[12,12],}",
          "{[10,10],[11,11],[12,12],[13,13],[14,14],}",
          "{[12,12],[13,13],[14,14],[15,15],[16,16],}",
          "{[14,14],[15,15],[16,16],[17,17],[18,18],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 10; i++) {
      srcData.putInt(i, i);
    }
    int windowRange = 5;
    int slideStep = 2;
    CountFixedFeatureExtractor countFixed =
        new CountFixedFeatureExtractor(TSDataType.INT32, windowRange, slideStep, true, true);
    countFixed.appendNewSrcData(srcData);
    while (countFixed.hasNext()) {
      countFixed.processNext();
    }
    countFixed.clearProcessedSrcData();
    ByteBuffer previous = countFixed.serializePrevious();
    countFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 10; i < 20; i++) {
      srcData2.putInt(i, i);
    }
    CountFixedFeatureExtractor countFixed2 =
        new CountFixedFeatureExtractor(TSDataType.INT32, windowRange, slideStep, true, true);
    countFixed2.deserializePrevious(previous);
    countFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(countFixed2, groundTruthL1, groundTruthL2);
    countFixed2.closeAndRelease();
  }

  private void assertL1AndL2(
      CountFixedFeatureExtractor countFixed, String[] groundTruthL1, String[] groundTruthL2)
      throws IOException {
    int idx = 0;
    while (countFixed.hasNext()) {
      System.out.println("idx:" + idx);
      countFixed.processNext();
      //      //L1 latest
      //      Identifier identifierL1 = countFixed.getCurrent_L1_Identifier();
      //      System.out.println(identifierL1);
      //      Assert.assertEquals(groundTruthL1[idx], identifierL1.toString());
      //      //L1 latest N, get data more than processed, it's expected to return only the
      // processed data.
      //      List<Identifier> L1s = countFixed.getLatestN_L1_Identifiers(idx + 5);
      //      for (int i = 0; i <= idx; i++) {
      //        System.out.println(L1s.get(i).toString());
      //        Assert.assertEquals(groundTruthL1[i], L1s.get(i).toString());
      //      }

      // L2 latest
      TVList seqL2 = (TVList) countFixed.getCurrent_L2_AlignedSequence();
      System.out.println(IndexTestUtils.tvListToString(seqL2));
      Assert.assertEquals(groundTruthL2[idx], IndexTestUtils.tvListToString(seqL2));
      // L2 latest N
      List<Object> L2s = countFixed.getLatestN_L2_AlignedSequences(idx + 5);
      for (int i = 0; i <= idx; i++) {
        System.out.println(IndexTestUtils.tvListToString((TVList) L2s.get(i)));
        Assert.assertEquals(groundTruthL2[i], IndexTestUtils.tvListToString((TVList) L2s.get(i)));
      }
      // release
      TVListAllocator.getInstance().release(seqL2);
      L2s.forEach(p -> TVListAllocator.getInstance().release((TVList) p));
      idx++;
    }
  }
}
