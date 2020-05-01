package org.apache.iotdb.db.index.algorithm.elb.pattern;

import java.util.Arrays;
import org.apache.iotdb.db.index.algorithm.elb.feature.PatternEnvelope;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;

/**
 * Pattern ELB feature.
 */
public abstract class ELBFeature {

  /**
   * upper and lower bounds without calculating the threshold
   */
  protected double[] actualUppers;
  protected double[] actualLowers;

  /**
   * upper and lower bounds considering the threshold. They are what we use in query.
   */
  protected double[] upperLines;
  protected double[] lowerLines;

  public boolean contains(double value, int i) {
    return upperLines[i] >= value && lowerLines[i] <= value;
  }

  protected void checkAndExpandArrays(int blockNum) {
    if (upperLines == null || upperLines.length < blockNum) {
      upperLines = new double[blockNum];
      lowerLines = new double[blockNum];
      actualUppers = new double[blockNum];
      actualLowers = new double[blockNum];
    }
    Arrays.fill(upperLines, -Double.MAX_VALUE);
    Arrays.fill(lowerLines, Double.MAX_VALUE);
    Arrays.fill(actualUppers, -Double.MAX_VALUE);
    Arrays.fill(actualLowers, Double.MAX_VALUE);
  }

  public abstract void refreshAndAppendToList(MilesPattern pattern, int blockNum,
      PatternEnvelope envelope, PrimitiveList mbrs);

  protected void appendToMBRs(PrimitiveList mbrs, int blockNum) {
    for (int i = 0; i < blockNum; i++) {
      mbrs.putDouble(upperLines[i]);
      mbrs.putDouble(lowerLines[i]);
    }
  }

}
