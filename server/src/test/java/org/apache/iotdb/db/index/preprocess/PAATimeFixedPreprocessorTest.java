/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package org.apache.iotdb.db.index.preprocess;
//
// import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
//
// import java.io.IOException;
// import java.nio.ByteBuffer;
// import java.util.List;
// import org.apache.iotdb.db.index.IndexTestUtils;
// import org.apache.iotdb.db.rescon.TVListAllocator;
// import org.apache.iotdb.db.utils.datastructure.TVList;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.junit.Assert;
// import org.junit.Test;
//
// public class PAATimeFixedPreprocessorTest {
//
//  @Test
//  public void testCreateAlignedSequence() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "[0-19,7]",
//        "[5-24,7]",
//        "[10-29,6]",
//        "[15-34,7]",
//        "[20-39,7]"
//    };
//    String[] groundTruthL2 = new String[]{
//        "{[0,1.50],[5,7.50],[10,12.00],[15,16.50],}",
//        "{[5,7.50],[10,12.00],[15,16.50],[20,22.50],}",
//        "{[10,12.00],[15,16.50],[20,22.50],[25,27.00],}",
//        "{[15,16.50],[20,22.50],[25,27.00],[30,31.50],}",
//        "{[20,22.50],[25,27.00],[30,31.50],[35,37.50],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 0; i < 15; i++) {
//      srcData.putInt(i * 3, i * 3);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 4;
//    int slideStep = 5;
//
//    PAATimeFixedFeatureExtractor timeFixedWithoutStored = new
// PAATimeFixedFeatureExtractor(TSDataType.INT32,
//        windowRange, slideStep, alignedSequenceLength, 0, false, false);
//    timeFixedWithoutStored.appendNewSrcData(srcData);
//    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
//    timeFixedWithoutStored.clear();
//  }
//
//  @Test
//  public void testNoMorePoints() throws IOException {
//    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 0; i < 3; i++) {
//      srcData.putInt(i * 15, i * 15);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 4;
//    int slideStep = 5;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(TSDataType.INT32,
// windowRange,
//        slideStep, alignedSequenceLength, 0, true, true);
//    timeFixed.appendNewSrcData(srcData);
//    Assert.assertFalse(timeFixed.hasNext());
//    timeFixed.clear();
//    System.out.println();
//    PAATimeFixedFeatureExtractor timeFixedWithoutStored = new
// PAATimeFixedFeatureExtractor(TSDataType.INT32,
//        windowRange, slideStep, alignedSequenceLength, 3, false, false);
//    timeFixedWithoutStored.appendNewSrcData(srcData);
//    Assert.assertFalse(timeFixedWithoutStored.hasNext());
//    timeFixedWithoutStored.clear();
//  }
//
//
//  private void assertL1AndL2(TimeFixedFeatureExtractor timeFixed, String[] groundTruthL1,
//      String[] groundTruthL2) throws IOException {
//    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2, true);
//  }
//
//  private void assertL1AndL2(TimeFixedFeatureExtractor timeFixed, String[] groundTruthL1,
//      String[] groundTruthL2, boolean toAssert) throws IOException {
//    int idx = 0;
//    while (timeFixed.hasNext()) {
//      System.out.println("idx:" + idx);
//      timeFixed.processNext();
//      //L1 latest
//      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
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
//
//      //L2 latest
//      TVList seqL2 = (TVList) timeFixed.getCurrent_L2_AlignedSequence();
//      System.out.println(IndexTestUtils.tvListToString(seqL2));
//      if (toAssert) {
//        Assert.assertEquals(groundTruthL2[idx], IndexTestUtils.tvListToString(seqL2));
//      }
//      //L2 latest N
//      List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(idx + 5);
//      for (int i = 0; i <= idx; i++) {
//        System.out.println(IndexTestUtils.tvListToString((TVList) L2s.get(i)));
//        if (toAssert) {
//          Assert.assertEquals(groundTruthL2[i], IndexTestUtils.tvListToString((TVList)
// L2s.get(i)));
//        }
//      }
//      //release
//      TVListAllocator.getInstance().release(seqL2);
//      L2s.forEach(p -> TVListAllocator.getInstance().release((TVList) p));
//      idx++;
//    }
//  }
//
//  @Test
//  public void testClearAndProcess() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "",
//        "",
//        "",
//        "[12-31,7]",
//        "[17-36,7]"
//    };
//    String[] groundTruthL2 = new String[]{
//        "",
//        "",
//        "",
//        "{[12,13.50],[17,19.50],[22,24.00],[27,28.50],}",
//        "{[17,19.50],[22,24.00],[27,28.50],[32,34.50],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 0; i < 15; i++) {
//      srcData.putInt(i * 3, i * 3);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 4;
//    int slideStep = 5;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(TSDataType.INT32,
// windowRange,
//        slideStep, alignedSequenceLength, 2, true, true);
//    timeFixed.appendNewSrcData(srcData);
//
//    timeFixed.hasNext();
//    timeFixed.processNext();
//    timeFixed.hasNext();
//    timeFixed.processNext();
//    timeFixed.hasNext();
//    timeFixed.processNext();
//    timeFixed.clear();
//    timeFixed.hasNext();
//    timeFixed.processNext();
//    timeFixed.hasNext();
//    timeFixed.processNext();
//    //L1 latest
//    Object L1s = timeFixed.getCurrent_L1_Identifier();
//    Assert.assertEquals(groundTruthL1[4], L1s.toString());
//
//    List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(4);
//    Assert.assertEquals(2, L2s.size());
//    Assert.assertEquals(groundTruthL2[3], IndexTestUtils.tvListToString((TVList) L2s.get(0)));
//    Assert.assertEquals(groundTruthL2[4], IndexTestUtils.tvListToString((TVList) L2s.get(1)));
//  }
//
//
//  @Test
//  public void testAlignedAndPrevious() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "[27-46,7]",
//        "[32-51,7]",
//        "[37-56,6]",
//        "[42-61,7]",
//        "[47-66,7]",
//        "[52-71,6]",
//        "[57-76,7]",
//        "[62-81,7]",
//        "[67-86,6]",
//    };
//    String[] groundTruthL2 = new String[]{
//        "{[27,28.50],[32,34.50],[37,39.00],[42,43.50],}",
//        "{[32,34.50],[37,39.00],[42,43.50],[47,49.50],}",
//        "{[37,39.00],[42,43.50],[47,49.50],[52,54.00],}",
//        "{[42,43.50],[47,49.50],[52,54.00],[57,58.50],}",
//        "{[47,49.50],[52,54.00],[57,58.50],[62,64.50],}",
//        "{[52,54.00],[57,58.50],[62,64.50],[67,69.00],}",
//        "{[57,58.50],[62,64.50],[67,69.00],[72,73.50],}",
//        "{[62,64.50],[67,69.00],[72,73.50],[77,79.50],}",
//        "{[67,69.00],[72,73.50],[77,79.50],[82,84.00],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
//    for (int i = 0; i < 15; i++) {
//      srcData.putInt(i * 3, i * 3);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 4;
//    int slideStep = 5;
//    int timeAnchor = 2;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(TSDataType.INT32,
// windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, true);
//    timeFixed.appendNewSrcData(srcData);
//    while (timeFixed.hasNext()) {
//      timeFixed.processNext();
//      Identifier identifierL1 = timeFixed.getCurrent_L1_Identifier();
//      System.out.println(identifierL1);
//    }
//    ByteBuffer previous = timeFixed.serializePrevious();
//    timeFixed.closeAndRelease();
//
//    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 15; i < 30; i++) {
//      srcData2.putInt(i * 3, i * 3);
//    }
//
//    PAATimeFixedFeatureExtractor timeFixed2 = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, true);
//    timeFixed2.deserializePrevious(previous);
//    timeFixed2.appendNewSrcData(srcData2);
//    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2, true);
//    timeFixed2.closeAndRelease();
//  }
//
//  @Test
//  public void testNoRestForNextOpen() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "[20-29,10]",
//        "[30-39,10]",
//    };
//    String[] groundTruthL2 = new String[]{
//        "{[20,20.50],[22,22.50],[24,24.50],[26,26.50],[28,28.50],}",
//        "{[30,30.50],[32,32.50],[34,34.50],[36,36.50],[38,38.50],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
//    for (int i = 0; i < 20; i++) {
//      srcData.putInt(i, i);
//    }
//    int windowRange = 10;
//    int alignedSequenceLength = 5;
//    int slideStep = 10;
//    int timeAnchor = 0;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, false);
//    timeFixed.appendNewSrcData(srcData);
//    while (timeFixed.hasNext()) {
//      timeFixed.processNext();
//      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
//      System.out.println(identifierL1);
//    }
//    ByteBuffer previous = timeFixed.serializePrevious();
//    timeFixed.closeAndRelease();
//
//    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 20; i < 40; i++) {
//      srcData2.putInt(i, i);
//    }
//
//    PAATimeFixedFeatureExtractor timeFixed2 = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, true);
//    timeFixed2.deserializePrevious(previous);
//    timeFixed2.appendNewSrcData(srcData2);
//    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2, false);
//    timeFixed2.closeAndRelease();
//  }
//
//
//  @Test
//  public void testMissingWindow() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "[27-46,7]",
//        "[32-51,7]",
//        "[37-56,6]",
//        "[42-61,6]",
//        "[67-86,6]",
//    };
//    String[] groundTruthL2 = new String[]{
//        "{[27,28.50],[32,34.50],[37,39.00],[42,43.50],}",
//        "{[32,34.50],[37,39.00],[42,43.50],[47,49.50],}",
//        "{[37,39.00],[42,43.50],[47,49.50],[52,54.00],}",
//        "{[42,43.50],[47,49.50],[52,54.00],[57,57.00],}",
//        "{[67,69.00],[72,73.50],[77,79.50],[82,84.00],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
//    for (int i = 0; i < 15; i++) {
//      srcData.putInt(i * 3, i * 3);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 4;
//    int slideStep = 5;
//    int timeAnchor = 2;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, false, false);
//    timeFixed.appendNewSrcData(srcData);
//    while (timeFixed.hasNext()) {
//      timeFixed.processNext();
//      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
//      System.out.println(identifierL1);
//    }
//    ByteBuffer previous = timeFixed.serializePrevious();
//    timeFixed.closeAndRelease();
//
//    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 15; i < 20; i++) {
//      srcData2.putInt(i * 3, i * 3);
//    }
//
//    for (int i = 23; i < 30; i++) {
//      srcData2.putInt(i * 3, i * 3);
//    }
//
//    PAATimeFixedFeatureExtractor timeFixed2 = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, true);
//    timeFixed2.deserializePrevious(previous.duplicate());
//    timeFixed2.appendNewSrcData(srcData2);
//    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2, true);
//    timeFixed2.closeAndRelease();
//  }
//
//  @Test
//  public void testUnDividable() throws IOException {
//    String[] groundTruthL1 = new String[]{
//        "[12-31,7]",
//        "[17-36,7]",
//        "[22-41,6]",
//        "[27-46,7]",
//        "[32-51,7]",
//        "[37-56,6]",
//    };
//    String[] groundTruthL2 = new String[]{
//        "{[12,12.00],[15,15.00],[18,18.00],[21,21.00],[24,24.00],[27,27.00],}",
//        "{[17,18.00],[20,21.00],[23,24.00],[26,27.00],[29,30.00],[32,33.00],}",
//        "{[22,24.00],[25,27.00],[28,30.00],[31,33.00],[34,36.00],[37,39.00],}",
//        "{[27,27.00],[30,30.00],[33,33.00],[36,36.00],[39,39.00],[42,42.00],}",
//        "{[32,33.00],[35,36.00],[38,39.00],[41,42.00],[44,45.00],[47,48.00],}",
//        "{[37,39.00],[40,42.00],[43,45.00],[46,48.00],[49,51.00],[52,54.00],}",
//    };
//    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
//    for (int i = 0; i < 10; i++) {
//      srcData.putInt(i * 3, i * 3);
//    }
//    int windowRange = 20;
//    int alignedSequenceLength = 6;
//    int slideStep = 5;
//    int timeAnchor = 2;
//    PAATimeFixedFeatureExtractor timeFixed = new PAATimeFixedFeatureExtractor(TSDataType.INT32,
// windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, false, false);
//    timeFixed.appendNewSrcData(srcData);
//    while (timeFixed.hasNext()) {
//      timeFixed.processNext();
//      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
//      System.out.println(identifierL1);
//    }
//    ByteBuffer previous = timeFixed.serializePrevious();
//    timeFixed.closeAndRelease();
//
//    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
//    for (int i = 10; i < 20; i++) {
//      srcData2.putInt(i * 3, i * 3);
//    }
//
//    PAATimeFixedFeatureExtractor timeFixed2 = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, false, false);
//    timeFixed2.deserializePrevious(previous.duplicate());
//    timeFixed2.appendNewSrcData(srcData2);
//    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2, true);
//    timeFixed2.closeAndRelease();
//
//    PAATimeFixedFeatureExtractor timeFixed3 = new PAATimeFixedFeatureExtractor(INT32, windowRange,
//        slideStep, alignedSequenceLength, timeAnchor, true, true);
//    timeFixed3.deserializePrevious(previous.duplicate());
//    timeFixed3.appendNewSrcData(srcData2);
//    assertL1AndL2(timeFixed3, groundTruthL1, groundTruthL2, true);
//    timeFixed3.closeAndRelease();
//  }
//
// }
