package org.apache.iotdb.db.index.algorithm.elb.feature.point;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.iotdb.db.index.algorithm.elb.feature.SequenceELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.feature.PatternEnvelope;
import org.apache.iotdb.db.index.algorithm.elb.feature.ElementELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LNormDouble;
import org.junit.Test;

/**
 * Created by kangrong on 17/1/3.
 */
public class PointPatternNodeTest {

  @Test
  public void test() {
    System.out.println(Arrays.deepToString(new int[][]{new int[]{1, 2}, new int[]{1, 2}}));
  }
//  @Test
//  public void nodeTest() {
//    double[] data = {1, 2, 4, 1, 9, 11};
//    int offset = 2;
//    int len = 3;
//    double thres = 0.5f;
//    ElementELBFeature node = new ElementELBFeature(data, offset, len, thres);
////        System.out.println(node);
//    assertEquals("upperLine,9.5,lowerLine,0.5,actualUpper:,9.0,actualLower,1.0", node.toString());
//  }
//
//  public PatternNode[] buildIndex(MilesPattern pattern, int windowBlockSize,
//      Distance disMetric, boolean useELE) {
//    if (useELE) {
//      PatternEnvelope envelope = new PatternEnvelope(pattern);
//      return ElementELBFeature.getNodesFromMultiTolerance(
//          false, pattern, windowBlockSize, envelope);
//    } else {
//      return SequenceELBFeature.getNodesFromMultiTolerance(
//          false, pattern, windowBlockSize, disMetric);
//    }
//
//  }
//
//  @Test
//  public void getNodesFromMultiToleranceTest() {
//    double[] dataPoints = new double[20];
//    for (int i = 0; i < 20; i++) {
//      dataPoints[i] = i;
//    }
//    int[] indexes = {0, 5, 12};
//    TolsPttGenerator3 generator = new TolsPttGenerator3(dataPoints, indexes, new LNormDouble(1));
////    ELBPatternParamGenerator generator = new ELBPatternParamGenerator(dataPoints, indexes, new LInfinityNormdouble());
//    MilesPattern pattern = generator.getDiffBoundRadius(0.2, 2);
//    PatternNode[] ele = buildIndex(pattern, 5, generator.distance, true);
//    PatternNode[] avg = buildIndex(pattern, 5, generator.distance, false);
//    System.out.println(Arrays.toString(ele));
//    System.out.println(Arrays.toString(avg));
//    String[] result = {"upperLine,6.0,lowerLine,-1.0,actualUpper:,4.0,actualLower,1.0",
//        "upperLine,8.0,lowerLine,1.0,actualUpper:,7.0,actualLower,3.0",
//        "upperLine,6.0,lowerLine,0.0,actualUpper:,5.0,actualLower,2.0"};
////    for (int i = 0; i < ret.length; i++) {
////      System.out.println(ret[i]);
////      assertEquals(result[i], ret[i].toString());
////    }
//  }
}