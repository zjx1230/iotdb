package org.apache.iotdb.db.index.preprocess;

import java.io.IOException;
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
        "[0-20,4]",
        "[5-25,4]",
        "[10-30,4]",
        "[15-35,4]",
        "[20-40,4]"
    };
    String[] groundTruthL2 = new String[]{
        "{[0,0],[5,6],[10,9],[15,15],}",
        "{[5,6],[10,9],[15,15],[20,21],}",
        "{[10,9],[15,15],[20,21],[25,24],}",
        "{[15,15],[20,21],[25,24],[30,30],}",
        "{[20,21],[25,24],[30,30],[35,36],}"
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(srcData, windowRange,
        alignedSequenceLength, slideStep, true, true);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2);
    timeFixed.clear();

    TimeFixedPreprocessor timeFixedWithoutStored = new TimeFixedPreprocessor(srcData, windowRange,
        alignedSequenceLength, slideStep, false, false);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
    timeFixedWithoutStored.clear();
  }

  @Test
  public void testCreateAlignedSequence2() throws IOException {
    String[] groundTruthL1 = new String[]{
        "[0-20,4]",
        "[5-25,4]",
        "[10-30,4]",
        "[15-35,4]",
        "[20-40,4]"
    };
    String[] groundTruthL2 = new String[]{
        "{[0,0],[5,0],[10,15],[15,15],}",
        "{[5,0],[10,15],[15,15],[20,15],}",
        "{[10,15],[15,15],[20,15],[25,30],}"
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 3; i++) {
      srcData.putInt(i * 15, i * 15);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(srcData, windowRange,
        alignedSequenceLength, slideStep, true, true);
    assertL1AndL2(timeFixed, groundTruthL1, groundTruthL2);
    timeFixed.clear();

    TimeFixedPreprocessor timeFixedWithoutStored = new TimeFixedPreprocessor(srcData, windowRange,
        alignedSequenceLength, slideStep, false, false);
    assertL1AndL2(timeFixedWithoutStored, groundTruthL1, groundTruthL2);
    timeFixedWithoutStored.clear();
  }

  private void assertL1AndL2(TimeFixedPreprocessor timeFixed, String[] groundTruthL1,
      String[] groundTruthL2) throws IOException {
    int idx = 0;
    while (timeFixed.hasNext()) {
//      System.out.println("idx:" + idx);
      timeFixed.processNext();
      //L1 latest
      Identifier identifierL1 = (Identifier) timeFixed.getCurrent_L1_Identifier();
//      System.out.println(identifier);
      Assert.assertEquals(groundTruthL1[idx], identifierL1.toString());
      //L1 latest N
      List<Object> L1s = timeFixed.getLatestN_L1_Identifiers(idx + 5);
      for (int i = 0; i <= idx; i++) {
        Assert.assertEquals(groundTruthL1[i], L1s.get(i).toString());
      }

      //L2 latest
      TVList seqL2 = (TVList) timeFixed.getCurrent_L2_AlignedSequence();
      Assert.assertEquals(groundTruthL2[idx], TestUtils.tvListToString(seqL2));
      //L2 latest N
      List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(idx + 5);
      for (int i = 0; i <= idx; i++) {
        Assert.assertEquals(groundTruthL2[i], TestUtils.tvListToString((TVList) L2s.get(i)));
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
        "[0-20,4]",
        "[5-25,4]",
        "[10-30,4]",
        "[15-35,4]",
        "[20-40,4]"
    };
    String[] groundTruthL2 = new String[]{
        "{[0,0],[5,6],[10,9],[15,15],}",
        "{[5,6],[10,9],[15,15],[20,21],}",
        "{[10,9],[15,15],[20,21],[25,24],}",
        "{[15,15],[20,21],[25,24],[30,30],}",
        "{[20,21],[25,24],[30,30],[35,36],}"
    };
    TVList srcData = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 15; i++) {
      srcData.putInt(i * 3, i * 3);
    }
    int windowRange = 20;
    int alignedSequenceLength = 4;
    int slideStep = 5;
    TimeFixedPreprocessor timeFixed = new TimeFixedPreprocessor(srcData, windowRange,
        alignedSequenceLength, slideStep, true, true);

    timeFixed.processNext();
    timeFixed.processNext();
    timeFixed.processNext();
    timeFixed.clear();
    timeFixed.processNext();
    timeFixed.processNext();
    //L1 latest
    Object L1s = timeFixed.getCurrent_L1_Identifier();
    Assert.assertEquals(groundTruthL1[4], L1s.toString());

    List<Object> L2s = timeFixed.getLatestN_L2_AlignedSequences(4);
    Assert.assertEquals(2, L2s.size());
    Assert.assertEquals(groundTruthL2[3], TestUtils.tvListToString((TVList) L2s.get(0)));
    Assert.assertEquals(groundTruthL2[4], TestUtils.tvListToString((TVList) L2s.get(1)));
  }
}
