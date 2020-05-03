package org.apache.iotdb.db.index.algorithm.paa;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iotdb.db.index.common.IndexRuntimeException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.TimeFixedPreprocessor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * PAA (Piecewise Aggregate Approximation), a classical feature in time series. <p>
 *
 * Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large time
 * series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAATimeFixedPreprocessor extends TimeFixedPreprocessor {

  public PAATimeFixedPreprocessor(TVList srcData, int windowRange, int slideStep, int paaDim,
      boolean storeIdentifier, boolean storeAligned) {
    super(srcData, windowRange, slideStep, paaDim, storeIdentifier, storeAligned);
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

  /**
   * custom for {@linkplain PAAIndex}
   *
   * @param currentCorners current corners
   */
  void copyFeature(float[] currentCorners) {
    if (currentAligned.size() != currentCorners.length) {
      throw new IndexRuntimeException(String.format("paa aligned.size %d != corners.length %d",
          currentAligned.size(), currentCorners.length));
    }
    for (int i = 0; i < currentCorners.length; i++) {
      currentCorners[i] = IndexUtils.getFloatFromAnyType(currentAligned, i);
    }
  }

  /**
   * custom for {@linkplain PAAIndex}
   *
   * @param idx the idx-th identifiers
   * @param outputStream to output
   */
  void serializeIdentifier(Integer idx, OutputStream outputStream) throws IOException {
    int actualIdx = idx - flushedOffset;
    if (actualIdx * 3 + 2 >= identifierList.size()) {
      throw new IOException(String.format("PAA serialize: idx %d*3+2 > identifiers size %d", idx,
          identifierList.size()));
    }
    if (!storeIdentifier) {
      throw new IOException("In PAA index, must store the identifier list");
    }
    ReadWriteIOUtils.write(identifierList.getLong(actualIdx * 3), outputStream);
    ReadWriteIOUtils.write(identifierList.getLong(actualIdx * 3 + 1), outputStream);
    ReadWriteIOUtils.write((int) identifierList.getLong(actualIdx * 3 + 2), outputStream);
  }
}
