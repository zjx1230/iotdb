package org.apache.iotdb.db.index.algorithm.elb.feature;

import org.apache.iotdb.db.index.algorithm.elb.pattern.ELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;

/**
 * PatternNode：Pattern被分割成的小block，lowerbound 1 Created by kangrong on 16/12/27.
 */
public class ElementELBFeature extends ELBFeature {

  /**
   * calculate ELE-ELB.
   */
  public void refreshAndAppendToList(MilesPattern pattern, int blockNum,
      PatternEnvelope envelope, PrimitiveList mbrs) {
    int windowBlockSize = (int) Math.floor(((double) pattern.sequenceLen) / blockNum);
    checkAndExpandArrays(blockNum);
    for (int i = 0; i < blockNum; i++) {
      // get the largest tolerances in this pattern
      for (int j = i * windowBlockSize; j < (i + 1) * windowBlockSize; j++) {
        if (envelope.upperLine[j] > upperLines[i]) {
          upperLines[i] = envelope.upperLine[j];
        }
        if (envelope.lowerLine[j] < lowerLines[i]) {
          lowerLines[i] = envelope.lowerLine[j];
        }
        if (envelope.valueLine[j] > actualUppers[i]) {
          actualUppers[i] = envelope.valueLine[j];
        }
        if (envelope.valueLine[j] < actualLowers[i]) {
          actualLowers[i] = envelope.valueLine[j];
        }
      }
    }

    appendToMBRs(mbrs, blockNum);
  }

}
