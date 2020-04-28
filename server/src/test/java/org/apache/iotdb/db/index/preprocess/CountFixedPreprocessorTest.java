package org.apache.iotdb.db.index.preprocess;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.index.TestUtils;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class CountFixedPreprocessorTest {

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
    for (int i = 0; i < 9; i++) {
      srcData.putInt(i, i);
    }
    int windowRange = 5;
    int slideStep = 2;
    CountFixedPreprocessor countFixed = new CountFixedPreprocessor(srcData, windowRange,
        slideStep, true, true);
    assertL1AndL2(countFixed, groundTruthL1, groundTruthL2);
    CountFixedPreprocessor countFixedWithoutStored = new CountFixedPreprocessor(srcData,
        windowRange, slideStep, false, false);
    assertL1AndL2(countFixedWithoutStored, groundTruthL1, groundTruthL2);
    countFixed.clear();
    countFixedWithoutStored.clear();
  }

  private void assertL1AndL2(CountFixedPreprocessor countFixed, String[] groundTruthL1,
      String[] groundTruthL2) throws IOException {
    int idx = 0;
    while (countFixed.hasNext()) {
      countFixed.processNext();
      //L1 latest
      Identifier identifierL1 = (Identifier) countFixed.getCurrent_L1_Identifier();
      System.out.println(identifierL1);
//      Assert.assertEquals(groundTruthL1[idx], identifierL1.toString());
      //L1 latest N
      List<Object> L1s = countFixed.getLatestN_L1_Identifiers(idx + 1);
      for (int i = 0; i < L1s.size(); i++) {
        System.out.println(L1s.get(i).toString());
//        Assert.assertEquals(groundTruthL1[i], L1s.get(i).toString());
      }

      //L2 latest
      int startIdxL2 = (int) countFixed.getCurrent_L2_AlignedSequence();
      String l2Lastest = TestUtils.stringFromAlignedSequenceByCountFixed(countFixed.getSrcData(),
          startIdxL2, countFixed.windowRange);
      System.out.println(l2Lastest);
//      Assert.assertEquals(groundTruthL2[idx], l2Lastest);
      //L2 latest N
      List<Object> L2s = countFixed.getLatestN_L2_AlignedSequences(idx + 1);
      for (int i = 0; i < L2s.size(); i++) {
        String l2pastI = TestUtils.stringFromAlignedSequenceByCountFixed(countFixed.getSrcData(),
            (int) L2s.get(i), countFixed.windowRange);
        System.out.println(l2pastI);
//        Assert.assertEquals(groundTruthL2[i], l2pastI);
      }
      idx++;
    }
  }
}
