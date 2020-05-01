package org.apache.iotdb.db.index.algorithm.elb.feature;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

/**
 * Test ELB-ELE envelope
 */
public class PatternEnvelopeTest {

  @Test
  public void testConstruct() {
    double[] dataPoints = {1, 4, 2, 3, 4, 7, 5, 4, 2};
    TVList tvList = TVList.newList(TSDataType.DOUBLE);
    assert tvList != null;
    for (int i = 0; i < dataPoints.length; i++) {
      tvList.putDouble(i, dataPoints[i]);
    }
    Distance distance = new LInfinityNormdouble();
    MilesPattern pattern = new MilesPattern(distance);
    PatternEnvelope envelope = new PatternEnvelope();
    int subpatternCount = 3;
    int seriesLength = dataPoints.length;
    double[] thresholdsArray = {2, 1, 2};
    int[] minLeftBorders = {0, 5, 7, seriesLength};
    int[] maxLeftBorders = {0, 5, 7, seriesLength};
    pattern.refresh(tvList, 0, seriesLength, subpatternCount, thresholdsArray, minLeftBorders,
        maxLeftBorders);
    envelope.refresh(pattern);

    assertEquals("[3.0, 6.0, 4.0, 5.0, 6.0, 8.0, 6.0, 6.0, 4.0]",
        Arrays.toString(envelope.upperLine));
    assertEquals("[1.0, 4.0, 2.0, 3.0, 4.0, 7.0, 5.0, 4.0, 2.0]",
        Arrays.toString(envelope.valueLine));
    assertEquals("[-1.0, 2.0, 0.0, 1.0, 2.0, 6.0, 4.0, 2.0, 0.0]",
        Arrays.toString(envelope.lowerLine));
  }

}