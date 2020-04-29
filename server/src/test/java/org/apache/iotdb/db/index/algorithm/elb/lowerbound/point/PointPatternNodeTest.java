package org.apache.iotdb.db.index.algorithm.elb.lowerbound.point;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.iotdb.db.index.algorithm.elb.NTolsPattern;
import org.apache.iotdb.db.index.algorithm.elb.lowerbound.PatternEnvelope;
import org.apache.iotdb.db.index.algorithm.elb.lowerbound.PatternNode;
import org.apache.iotdb.db.index.algorithm.elb.lowerbound.TolsPointPatternNode;
import org.apache.iotdb.db.index.algorithm.elb.lowerbound.ELBPatternParamGenerator;
import org.apache.iotdb.db.index.algorithm.elb.lowerbound.NTolsAvgPatternNode;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.index.distance.LNormdouble;
import org.junit.Test;

/**
 * Created by kangrong on 17/1/3.
 */
public class PointPatternNodeTest {

  @Test
  public void nodeTest() {
    double[] data = {1, 2, 4, 1, 9, 11};
    int offset = 2;
    int len = 3;
    double thres = 0.5f;
    TolsPointPatternNode node = new TolsPointPatternNode(data, offset, len, thres);
//        System.out.println(node);
    assertEquals("upperLine,9.5,lowerLine,0.5,actualUpper:,9.0,actualLower,1.0", node.toString());
  }

  public PatternNode[] buildIndex(NTolsPattern pattern, int windowBlockSize,
      Distance disMetric, boolean useELE) {
    if (useELE) {
      PatternEnvelope envelope = new PatternEnvelope(pattern);
      return TolsPointPatternNode.getNodesFromMultiTolerance(
          false, pattern, windowBlockSize, envelope);
    } else {
      return NTolsAvgPatternNode.getNodesFromMultiTolerance(
          false, pattern, windowBlockSize, disMetric);
    }

  }

  @Test
  public void getNodesFromMultiToleranceTest() {
    double[] dataPoints = new double[20];
    for (int i = 0; i < 20; i++) {
      dataPoints[i] = i;
    }
    int[] indexes = {0, 5, 12};
    ELBPatternParamGenerator generator = new ELBPatternParamGenerator(dataPoints, indexes, new LNormdouble(1));
//    ELBPatternParamGenerator generator = new ELBPatternParamGenerator(dataPoints, indexes, new LInfinityNormdouble());
    NTolsPattern pattern = generator.getDiffBoundRadius(0.2, 2);
    PatternNode[] ele = buildIndex(pattern, 5, generator.distance, true);
    PatternNode[] avg = buildIndex(pattern, 5, generator.distance, false);
    System.out.println(Arrays.toString(ele));
    System.out.println(Arrays.toString(avg));
    String[] result = {"upperLine,6.0,lowerLine,-1.0,actualUpper:,4.0,actualLower,1.0",
        "upperLine,8.0,lowerLine,1.0,actualUpper:,7.0,actualLower,3.0",
        "upperLine,6.0,lowerLine,0.0,actualUpper:,5.0,actualLower,2.0"};
//    for (int i = 0; i < ret.length; i++) {
//      System.out.println(ret[i]);
//      assertEquals(result[i], ret[i].toString());
//    }
  }
}