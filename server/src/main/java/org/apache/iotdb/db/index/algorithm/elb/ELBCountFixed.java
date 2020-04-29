package org.apache.iotdb.db.index.algorithm.elb;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.common.IllegalIndexParamException;
import org.apache.iotdb.db.index.preprocess.CountFixedPreprocessor;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * ELB (Equal-Length Block), a feature for efficient adjacent sequence pruning. <p>
 *
 * Refer to: Kang R, et al. Matching Consecutive Subpatterns over Streaming Time Series[C]
 * APWeb-WAIM Joint International Conference. Springer, Cham, 2018: 90-105.
 */
public class ELBCountFixed extends CountFixedPreprocessor {

  private final int blockNum;
  private final int blockWidth;
  /**
   * A list of MBRs. Every MBR contains {@code b} upper/lower bounds, i.e. {@code 2*b} doubles.<p>
   *
   * Format: {@code {u_11, l_11, ..., u_1b, l_1b; u_21, l_21, ..., u_2b, l_2b; ...}}
   */
  private final PrimitiveList mbrs;
//  /**
//   * Record the aligned sequences covered in a MBR. For one MBR, the covered sequences are adjacent.
//   * For each subsequence, we only save the idx of its first point in {@code srcData}.<p>
//   *
//   * You can reconstruct aligned sequences in form of {@linkplain TVList} by calling {@linkplain
//   * #constructAlignedSequencesInABR(int)} <p>
//   *
//   * Format: {@code startIdx_1, num_1, startIdx_1, num_2, ...}
//   */
//  private final PrimitiveList startIdxInMBR;
//  /**
//   * current number of generated MBRs.
//   */
//  private int numOfMBR;

  /**
   * ELB divides the aligned sequence into {@code b} equal-length blocks. For each block, ELB
   * calculates a float number pair as the upper and lower bounds.<p>
   *
   * A block contains {@code windowRange/b} points. A list of blocks ({@code b} blocks) cover
   * adjacent {@code windowRange/b} sequence.
   */
  public ELBCountFixed(TVList srcData, int windowRange, int slideStep, int b,
      boolean storeIdentifier, boolean storeAligned) {
    super(srcData, windowRange, slideStep, storeIdentifier, storeAligned);
    if (b > windowRange) {
      throw new IllegalIndexParamException(String
          .format("In PAA, blockNum %d cannot be larger than windowRange %d", b, windowRange));
    }
    this.blockNum = b;
    this.blockWidth = windowRange / b;
    this.mbrs = PrimitiveList.newList(TSDataType.DOUBLE);
//    this.startIdxInMBR = PrimitiveList.newList(TSDataType.INT32);
//    accumulativeNum = 0;
//    sizeOfMBR = 0;
  }

//  /**
//   * return the aligned sequences covered in the {@code mbrIdx}-th MBR.
//   *
//   * @param mbrIdx the idx of MBR
//   * @return covered sequences. The caller needs to release them outside.
//   */
//  public List<TVList> constructAlignedSequencesInABR(int mbrIdx) {
//    int startIdx = startIdxInMBR.getInt(mbrIdx * 2);
//    int seqNum = startIdxInMBR.getInt(mbrIdx * 2 + 1);
//    List<TVList> res = new ArrayList<>(seqNum);
//
//    for (int i = 0; i < seqNum; i++) {
//      res.add(createAlignedSequence(startIdx));
//      startIdx += slideStep;
//    }
//    return res;
//  }

  /**
   * The implementation of SEQ-ELB
   */
  private void calculateSeqBasedELB() {

  }

  @Override
  public void processNext() {
    super.processNext();
    calculateSeqBasedELB();

    currentProcessedIdx++;
    currentStartTimeIdx += slideStep;
    throw new Error();
//    currentStartTime = srcData.getTime(currentStartTimeIdx);
//    currentEndTime = srcData.getTime(currentStartTimeIdx + windowRange - 1);
//
//    // calculate the newest aligned sequence
//    if (storeIdentifier) {
//      // it's a naive identifier, we can refine it in the future.
//      identifierList.putLong(currentStartTime);
//      identifierList.putLong(currentEndTime);
//      identifierList.putLong(windowRange);
//    }
//    if (storeAligned) {
//      alignedList.putInt(currentStartTimeIdx);
//    }
  }


  @Override
  public List<Object> getLatestN_L3_Features(int latestN) {
    List<Object> res = new ArrayList<>(latestN);

    int startIdxPastN = Math.max(0, currentStartTimeIdx - (latestN - 1) * slideStep);
    while (startIdxPastN <= currentStartTimeIdx) {
      res.add(createAlignedSequence(startIdxPastN));
      startIdxPastN += slideStep;
    }
    return res;
  }

}
