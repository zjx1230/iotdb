package org.apache.iotdb.db.index.algorithm.elb;

import static org.apache.iotdb.db.index.common.IndexUtils.getDataTypeSize;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.pattern.CalcParam;
import org.apache.iotdb.db.index.common.IllegalIndexParamException;
import org.apache.iotdb.db.index.distance.Distance;
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
public class ELBCountFixedPreprocessor extends CountFixedPreprocessor {

  private final int blockNum;
  /**
   * A list of MBRs. Every MBR contains {@code b} upper/lower bounds, i.e. {@code 2*b} doubles.<p>
   *
   * Format: {@code {u_11, l_11, ..., u_1b, l_1b; u_21, l_21, ..., u_2b, l_2b; ...}}
   */
  private final PrimitiveList mbrs;
  private final ELBFeatureExtractor elbFeatureExtractor;

  /**
   * ELB divides the aligned sequence into {@code b} equal-length blocks. For each block, ELB
   * calculates a float number pair as the upper and lower bounds.<p>
   *
   * A block contains {@code windowRange/b} points. A list of blocks ({@code b} blocks) cover
   * adjacent {@code windowRange/b} sequence.
   */
  public ELBCountFixedPreprocessor(TVList srcData, int windowRange, int slideStep, int blockNum,
      Distance distance, CalcParam calcParam, ELBType elbType, boolean storeIdentifier,
      boolean storeAligned) {
    super(srcData, windowRange, slideStep, storeIdentifier, storeAligned);
    if (blockNum > windowRange) {
      throw new IllegalIndexParamException(String
          .format("In PAA, blockNum %d cannot be larger than windowRange %d", blockNum,
              windowRange));
    }
    this.blockNum = blockNum;
    this.mbrs = PrimitiveList.newList(TSDataType.DOUBLE);
    elbFeatureExtractor = new ELBFeatureExtractor(srcData, distance, windowRange, calcParam,
        blockNum, elbType);
  }

  public ELBCountFixedPreprocessor(TVList srcData, int windowRange, int slideStep, int blockNum,
      Distance distance, CalcParam calcParam, ELBType elbType) {
    this(srcData, windowRange, slideStep, blockNum, distance, calcParam, elbType, false, false);
  }

  @Override
  public void processNext() {
    super.processNext();
    elbFeatureExtractor.calcELBFeature(currentStartTimeIdx, mbrs);
  }

  private double[][] formatELBFeature(int processedIdx) {
    double[][] res = new double[blockNum][2];
    for (int i = 0; i < blockNum; i++) {
      res[i][0] = mbrs.getDouble(2 * blockNum * processedIdx + 2 * i);
      res[i][1] = mbrs.getDouble(2 * blockNum * processedIdx + 2 * i + 1);
    }
    return res;
  }

  @Override
  public List<Object> getLatestN_L3_Features(int latestN) {
    latestN = Math.min(getCurrentChunkSize(), latestN);
    List<Object> res = new ArrayList<>(latestN);
    if (latestN == 0) {
      return res;
    }
    int startIdx = Math.max(flushedOffset, currentProcessedIdx + 1 - latestN);
    for (int i = startIdx; i <= currentProcessedIdx; i++) {
      res.add(formatELBFeature(i - flushedOffset));
    }
    return res;
  }

  @Override
  public long clear() {
    long toBeReleased = super.clear();
    toBeReleased += mbrs.size() * getDataTypeSize(mbrs);
    mbrs.clearAndRelease();
    return toBeReleased;
  }

  @Override
  public int getAmortizedSize() {
    int res = super.getAmortizedSize();
    res += elbFeatureExtractor.getAmortizedSize();
    return res;
  }

}
