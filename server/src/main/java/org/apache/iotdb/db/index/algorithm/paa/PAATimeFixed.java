package org.apache.iotdb.db.index.algorithm.paa;

import org.apache.iotdb.db.index.preprocess.TimeFixedPreprocessor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * PAA (Piecewise Aggregate Approximation), a classical feature in time series. <p>
 *
 * Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large time
 * series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAATimeFixed extends TimeFixedPreprocessor {

  public PAATimeFixed(TVList srcData, int windowRange, int paaDim, int slideStep,
      boolean storeIdentifier, boolean storeAligned) {
    super(srcData, windowRange, paaDim, slideStep, storeIdentifier, storeAligned);
  }


  /**
   * Find the idx of the minimum timestamp greater than or equal to {@code targetTimestamp}.  If
   * not, return the idx of the timestamp closest to {@code targetTimestamp}.
   *
   * @param curIdx the idx to start scanning
   * @param targetTimestamp the target
   * @return the idx of the minimum timestamp >= {@code targetTimestamp}
   */
  @Override
  protected int locatedIdxToTimestamp(int curIdx, long targetTimestamp) {
    while (curIdx < srcData.size() - 1 && srcData.getTime(curIdx) < targetTimestamp) {
      curIdx++;
    }
    return curIdx;
  }

  /**
   * Use AVERAGE ALIGN to calculate PAA feature.
   *
   * @param startIdx the idx from which we start to search the closest timestamp.
   * @param leftBorderTimestamp the left border of sequence to be aligned.
   */
  @Override
  protected TVList createAlignedSequence(long leftBorderTimestamp, int startIdx) {
    TVList seq = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
    for (int i = 0; i < alignedDim; i++) {
      startIdx = locatedIdxToTimestamp(startIdx, leftBorderTimestamp);
      int endIdx = locatedIdxToTimestamp(startIdx, leftBorderTimestamp + intervalWidth);
      if (endIdx == startIdx) {
        // If there is no next timestamp, the current interval calculates at least one data point
        endIdx++;
      }
      float sum = 0;
      for (int idx = startIdx; idx < endIdx; idx++) {
        switch (srcData.getDataType()) {
          case INT32:
            sum += srcData.getInt(idx);
            break;
          case INT64:
            sum += srcData.getLong(idx);
            break;
          case FLOAT:
            sum += srcData.getFloat(idx);
            break;
          case DOUBLE:
            sum += srcData.getDouble(idx);
            break;
          default:
            throw new NotImplementedException(srcData.getDataType().toString());
        }
      }
      seq.putFloat(leftBorderTimestamp, sum / (endIdx - startIdx));
      leftBorderTimestamp += intervalWidth;
    }
    return seq;
  }

}
