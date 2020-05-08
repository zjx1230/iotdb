package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.db.index.TestUtils;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class TimeFixedPreprocessorTest {

  @Test
  public void testCreateAlignedSequence() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[0-19,4]",
        "[5-24,4]",
        "[10-29,4]",
        "[15-34,4]",
        "[20-39,4]"
    };
    String[] groundTruthL2 = new String[]{
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
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, 0, true, true);
    timeFixed.appendNewSrcData(srcData);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2);
    timeFixed.clear();

    TimeFixedPreprocessor timeFixedWithoutStored = new TimeFixedPreprocessor(INT32,
        windowRange, slideStep, alignedSequenceLength, 0, false, false);
    timeFixedWithoutStored.appendNewSrcData(srcData);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
    timeFixedWithoutStored.clear();
  }

  @Test
  public void testCreateAlignedSequence2() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[0-19,4]",
        "[5-24,4]",
        "[10-29,4]",
        "[15-34,4]",
        "[20-39,4]",
        "[25-44,4]",
        "[30-49,4]",
    };
    String[] groundTruthL2 = new String[]{
        "{[0,0],[5,15],[10,15],[15,15],}",
        "{[5,15],[10,15],[15,15],[20,15],}",
        "{[10,15],[15,15],[20,15],[25,15],}",
        "{[15,15],[20,30],[25,30],[30,30],}",
        "{[20,30],[25,30],[30,30],[35,30],}",
        "{[25,30],[30,30],[35,30],[40,30],}",
        "{[30,30],[35,30],[40,30],[45,30],}",
    };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 3; i++) {
      srcData.putInt(i * 15, i * 15);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, 0, true, true);
    timeFixed.appendNewSrcData(srcData);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2);
    timeFixed.clear();

    TimeFixedPreprocessor timeFixedWithoutStored = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, 0, false, false);
    timeFixedWithoutStored.appendNewSrcData(srcData);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
    timeFixedWithoutStored.clear();
  }

  @Test
  public void testAlignedAndPrevious() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[27-46,4]",
        "[32-51,4]",
        "[37-56,4]",
        "[42-61,4]",
        "[47-66,4]",
        "[52-71,4]",
        "[57-76,4]",
        "[62-81,4]",
        "[67-86,4]",
    };
    String[] groundTruthL2 = new String[]{
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
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      System.out.println(identifierL1);
    }
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 15; i < 30; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    TimeFixedPreprocessor timeFixed2 = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }

  @Test
  public void testNoRestForNextOpen() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[20-29,5]",
        "[30-39,5]",
    };
    String[] groundTruthL2 = new String[]{
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
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      System.out.println(identifierL1);
    }
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 20; i < 40; i++) {
      srcData2.putInt(i, i);
    }

    TimeFixedPreprocessor timeFixed2 = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }


  @Test
  public void testMissingWindow() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[27-46,4]",
        "[32-51,4]",
        "[37-56,4]",
        "[42-61,4]",
        "[47-66,4]",
        "[52-71,4]",
        "[57-76,4]",
        "[67-86,4]",
    };
    String[] groundTruthL2 = new String[]{
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
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed.appendNewSrcData(srcData);
    while (timeFixed.hasNext()) {
      timeFixed.processNext();
      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      System.out.println(identifierL1);
    }
    ByteBuffer previous = timeFixed.serializePrevious();
    timeFixed.closeAndRelease();

    TVList srcData2 = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 15; i < 20; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    for (int i = 28; i < 30; i++) {
      srcData2.putInt(i * 3, i * 3);
    }

    TimeFixedPreprocessor timeFixed2 = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, timeAnchor, true, true);
    timeFixed2.deserializePrevious(previous);
    timeFixed2.appendNewSrcData(srcData2);
    assertL1AndL2(timeFixed2, groundTruthL1, groundTruthL2);
    timeFixed2.closeAndRelease();
  }

  private void assertL1AndL2(TimeFixedPreprocessor timeFixed, String[] groundTruthL1,
      String[] groundTruthL2) throws IOException {
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2, true);
  }

  private void assertL1AndL2(TimeFixedPreprocessor timeFixed, String[] groundTruthL1,
      String[] groundTruthL2, boolean toAssert) throws IOException {
    int idx = 0;
    while (timeFixed.hasNext()) {
      System.out.println("idx:" + idx);
      timeFixed.processNext();
      //L1 latest
      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
      System.out.println(identifierL1);
      if (toAssert) {
        Assert.assertEquals(groundTruthL1[idx], identifierL1.toString());
      }
      //L1 latest N
      List<Object> L1s = timeFixed.getLatestN_L1_Identifiers(idx + 5);
      for (int i = 0; i <= idx; i++) {
        System.out.println(L1s.get(i).toString());
        if (toAssert) {
          Assert.assertEquals(groundTruthL1[i], L1s.get(i).toString());
        }
      }

      //L2 latest
      TVList seqL2 = (TVList) timeFixed.getCurrent_L2_AlignedSequence();
      System.out.println(TestUtils.tvListToString(seqL2));
      if (toAssert) {
        Assert.assertEquals(groundTruthL2[idx], TestUtils.tvListToString(seqL2));
      }
      //L2 latest N
      List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(idx + 5);
      for (int i = 0; i <= idx; i++) {
        System.out.println(TestUtils.tvListToString((TVList) L2s.get(i)));
        if (toAssert) {
          Assert.assertEquals(groundTruthL2[i], TestUtils.tvListToString((TVList) L2s.get(i)));
        }
      }
      //release
      TVListAllocator.getInstance().release(seqL2);
      L2s.forEach(p -> TVListAllocator.getInstance().release((TVList) p));
      idx++;
    }
  }

  @Test
  public void testClearAndProcess() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[-2-17,4]",
        "[3-22,4]",
        "[8-27,4]",
        "[13-32,4]",
        "[18-37,4]"
    };
    String[] groundTruthL2 = new String[]{
        "",
        "",
        "",
        "{[13,15],[18,18],[23,24],[28,30],}",
        "{[18,18],[23,24],[28,30],[33,33],}"
    };
    TVList srcData = TVListAllocator.getInstance().allocate(INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(INT32, windowRange,
        slideStep, alignedSequenceLength, 3, true, true);
    timeFixed.appendNewSrcData(srcData);
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.clear();
    timeFixed.hasNext();
    timeFixed.processNext();
    timeFixed.hasNext();
    timeFixed.processNext();
    //L1 latest
    Object L1s = timeFixed.getCurrent_L1_Identifier();
    System.out.println(L1s.toString());
    Assert.assertEquals(groundTruthL1[4], L1s.toString());

    List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(4);
    System.out.println(TestUtils.tvListToString((TVList) L2s.get(0)));
    System.out.println(TestUtils.tvListToString((TVList) L2s.get(1)));
    Assert.assertEquals(2, L2s.size());
    Assert.assertEquals(groundTruthL2[3], TestUtils.tvListToString((TVList) L2s.get(0)));
    Assert.assertEquals(groundTruthL2[4], TestUtils.tvListToString((TVList) L2s.get(1)));
  }
}
