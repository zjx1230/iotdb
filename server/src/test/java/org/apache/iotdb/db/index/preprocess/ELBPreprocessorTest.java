package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.db.index.TestUtils.TwoDimDoubleArrayToString;

import java.util.List;
import org.apache.iotdb.db.index.algorithm.elb.ELBCountFixedPreprocessor;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.pattern.CalcParam;
import org.apache.iotdb.db.index.algorithm.elb.pattern.SinglePattern;
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
    CalcParam calcParam = new SinglePattern(-1, 0.2, windowRange);
    ELBCountFixedPreprocessor elbProcessor = new ELBCountFixedPreprocessor(srcData,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.SEQ);
    assertL3(elbProcessor, groundTruthL3);
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
    CalcParam calcParam = new SinglePattern(-1, 0.2, windowRange);
    ELBCountFixedPreprocessor elbProcessor = new ELBCountFixedPreprocessor(srcData,
        windowRange, slideStep, blockNum, distance, calcParam, ELBType.ELE);
    assertL3(elbProcessor, groundTruthL3);
    elbProcessor.clear();
  }

  private void assertL3(ELBCountFixedPreprocessor elbProcessor, String[] groundTruthL3) {
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
      for (int i = 0; i <= idx; i++) {
        double[][] elb = (double[][]) L3s.get(i);
//        System.out.println(TwoDimDoubleArrayToString(elb));
        Assert.assertEquals(groundTruthL3[i], TwoDimDoubleArrayToString(elb));
      }
      idx++;
    }
  }
}
