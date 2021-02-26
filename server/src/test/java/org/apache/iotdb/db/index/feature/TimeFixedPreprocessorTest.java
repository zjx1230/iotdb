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

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;

public class TimeFixedPreprocessorTest {

  @Test
  public void testCreateAlignedSequence() throws IOException {
    String[] groundTruthL1 =
        new String[] {"[0-19,7]", "[5-24,7]", "[10-29,6]", "[15-34,7]", "[20-39,7]"};
    String[] groundTruthL2 =
        new String[] {
          "{[0,0],[5,6],[10,12],[15,15],}",
          "{[5,6],[10,12],[15,15],[20,21],}",
          "{[10,12],[15,15],[20,21],[25,27],}",
          "{[15,15],[20,21],[25,27],[30,30],}",
          "{[20,21],[25,27],[30,30],[35,36],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, 0, true, true);
    timeFixed.appendNewSrcData(srcData);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2);
    timeFixed.closeAndRelease();

    TimeFixedFeatureExtractor timeFixedWithoutStored =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, 0, false, false);
    timeFixedWithoutStored.appendNewSrcData(srcData);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
    timeFixedWithoutStored.closeAndRelease();
  }

  @Test
  public void testCreateAlignedSequence2() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[0-19,2]", "[5-24,1]", "[10-29,1]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[0,0],[5,15],[10,15],[15,15],}",
          "{[5,15],[10,15],[15,15],[20,15],}",
          "{[10,15],[15,15],[20,15],[25,15],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 3; i++) {
      srcData.putInt(i * 15, i * 15);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, 0, true, true);
    timeFixed.appendNewSrcData(srcData);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2, true);
    timeFixed.closeAndRelease();

    TimeFixedFeatureExtractor timeFixedWithoutStored =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, 0, false, false);
    timeFixedWithoutStored.appendNewSrcData(srcData);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2, true);
    timeFixedWithoutStored.closeAndRelease();
  }

  @Test
  public void testAlignedAndPrevious() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[27-46,7]",
          "[32-51,7]",
          "[37-56,6]",
          "[42-61,7]",
          "[47-66,7]",
          "[52-71,6]",
          "[57-76,7]",
          "[62-81,7]",
          "[67-86,6]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[27,27],[32,33],[37,39],[42,42],}",
          "{[32,33],[37,39],[42,42],[47,48],}",
          "{[37,39],[42,42],[47,48],[52,54],}",
          "{[42,42],[47,48],[52,54],[57,57],}",
          "{[47,48],[52,54],[57,57],[62,63],}",
          "{[52,54],[57,57],[62,63],[67,69],}",
          "{[57,57],[62,63],[67,69],[72,72],}",
          "{[62,63],[67,69],[72,72],[77,78],}",
          "{[67,69],[72,72],[77,78],[82,84],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    int timeAnchor = 2;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      //      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      //      System.out.println(identifierL1);
    }
    timeFixed.clearProcessedSrcData();
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 15; i < 30; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    TimeFixedFeatureExtractor timeFixed2 =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }

  @Test
  public void testNoRestForNextOpen() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[20-29,10]", "[30-39,10]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[20,20],[22,22],[24,24],[26,26],[28,28],}",
          "{[30,30],[32,32],[34,34],[36,36],[38,38],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 20; i++) {
      srcData.putInt(i, i);
    }
    int windowRange = 10;
    int alignedSequenceLength = 5;
    int slideStep = 10;
    int timeAnchor = 0;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      //      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      //      System.out.println(identifierL1);
    }
    timeFixed.clearProcessedSrcData();
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 20; i < 40; i++) {
      srcData2.putInt(i, i);
    }

    TimeFixedFeatureExtractor timeFixed2 =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }

  @Test
  public void testMissingWindow() throws IOException {
    String[] groundTruthL1 =
        new String[] {
          "[27-46,7]",
          "[32-51,7]",
          "[37-56,6]",
          "[42-61,6]",
          "[47-66,4]",
          "[52-71,2]",
          "[57-76,1]",
          "[67-86,1]",
        };
    String[] groundTruthL2 =
        new String[] {
          "{[27,27],[32,33],[37,39],[42,42],}",
          "{[32,33],[37,39],[42,42],[47,48],}",
          "{[37,39],[42,42],[47,48],[52,54],}",
          "{[42,42],[47,48],[52,54],[57,57],}",
          "{[47,48],[52,54],[57,57],[62,57],}",
          "{[52,54],[57,57],[62,57],[67,57],}",
          "{[57,57],[62,57],[67,57],[72,57],}",
          "{[67,84],[72,84],[77,84],[82,84],}",
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    int timeAnchor = 2;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      //      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      //      System.out.println(identifierL1);
    }
    timeFixed.clearProcessedSrcData();
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 15; i < 20; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    for (int i = 28; i < 30; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    TimeFixedFeatureExtractor timeFixed2 =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }

  private void assertL1AndL2(
      TimeFixedFeatureExtractor timeFixed, String[] groundTruthL1, String[] groundTruthL2)
      throws IOException {
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2, true);
  }

  private void assertL1AndL2(
      TimeFixedFeatureExtractor timeFixed,
      String[] groundTruthL1,
      String[] groundTruthL2,
      boolean toAssert)
      throws IOException {
    int idx = 0;
    while (timeFixed.hasNext()) {
      System.out.println("idx:" + idx);
      timeFixed.processNext();
      //      //L1 latest
      //      Identifier identifierL1 = timeFixed.getCurrent_L1_Identifier();
      //      System.out.println(identifierL1);
      //      if (toAssert) {
      //        Assert.assertEquals(groundTruthL1[idx], identifierL1.toString());
      //      }
      //      //L1 latest N
      //      List<Identifier> L1s = timeFixed.getLatestN_L1_Identifiers(idx + 5);
      //      for (int i = 0; i <= idx; i++) {
      //        System.out.println(L1s.get(i).toString());
      //        if (toAssert) {
      //          Assert.assertEquals(groundTruthL1[i], L1s.get(i).toString());
      //        }
      //      }

      // L2 latest
      TVList seqL2 = (TVList) timeFixed.getCurrent_L2_AlignedSequence();
      System.out.println(IndexTestUtils.tvListToString(seqL2));
      if (toAssert) {
        Assert.assertEquals(groundTruthL2[idx], IndexTestUtils.tvListToString(seqL2));
      }
      // L2 latest N
      List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(idx + 5);
      for (int i = 0; i <= idx; i++) {
        System.out.println(IndexTestUtils.tvListToString((TVList) L2s.get(i)));
        if (toAssert) {
          Assert.assertEquals(groundTruthL2[i], IndexTestUtils.tvListToString((TVList) L2s.get(i)));
        }
      }
      // release
      TVListAllocator.getInstance().release(seqL2);
      L2s.forEach(p -> TVListAllocator.getInstance().release((TVList) p));
      idx++;
    }
  }

  @Test
  public void testClearAndProcess() throws IOException {
    String[] groundTruthL1 = new String[] {"", "", "", "[13-32,6]", "[18-37,7]"};
    String[] groundTruthL2 =
        new String[] {
          "", "", "", "{[13,15],[18,18],[23,24],[28,30],}", "{[18,18],[23,24],[28,30],[33,33],}"
        };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedFeatureExtractor timeFixed =
        new TimeFixedFeatureExtractor(
            INT32, windowRange, slideStep, alignedSequenceLength, 3, true, true);
    timeFixed.appendNewSrcData(srcData);
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    // L1 latest
    //    Object L1s = timeFixed.getCurrent_L1_Identifier();
    //    System.out.println(L1s.toString());
    //    Assert.assertEquals(groundTruthL1[4], L1s.toString());

    List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(2);
    System.out.println(IndexTestUtils.tvListToString((TVList) L2s.get(0)));
    System.out.println(IndexTestUtils.tvListToString((TVList) L2s.get(1)));
    Assert.assertEquals(2, L2s.size());
    Assert.assertEquals(groundTruthL2[3], IndexTestUtils.tvListToString((TVList) L2s.get(0)));
    Assert.assertEquals(groundTruthL2[4], IndexTestUtils.tvListToString((TVList) L2s.get(1)));
  }
}
