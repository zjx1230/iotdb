package org.apache.iotdb.db.index.algorithm.paa;

import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.algorithm.MBRIndex;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.UnsupportedIndexQueryException;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.func.IndexFuncFactory;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Use PAA as the feature of MBRIndex.</p>
 */

public class PAAIndex extends MBRIndex {

  private static final Logger logger = LoggerFactory.getLogger(PAAIndex.class);

  private PAATimeFixedPreprocessor paaTimeFixedPreprocessor;

  public PAAIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo, true);
  }

  @Override
  public void initPreprocessor(ByteBuffer previous) {
    if (this.indexPreprocessor != null) {
      this.indexPreprocessor.clear();
    }
    this.paaTimeFixedPreprocessor = new PAATimeFixedPreprocessor(tsDataType, windowRange, slideStep,
        featureDim, confIndexStartTime, true, false);
    paaTimeFixedPreprocessor.deserializePrevious(previous);
    this.indexPreprocessor = paaTimeFixedPreprocessor;
  }

  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  @Override
  protected int fillCurrentFeature() {
    paaTimeFixedPreprocessor.copyFeature(currentCorners);
    Arrays.fill(currentRanges, 0);
    return paaTimeFixedPreprocessor.getSliceNum() - 1;
  }

  @Override
  protected BiConsumer<Integer, OutputStream> getSerializeFunc() {
    return (idx, outputStream) -> {
      try {
        paaTimeFixedPreprocessor.serializeIdentifier(idx, outputStream);
      } catch (IOException e) {
        logger.error("serialized error.", e);
      }
    };
  }


  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initQuery(Map<String, String> queryConditions, List<IndexFuncResult> indexFuncResults)
      throws UnsupportedIndexQueryException {
    for (IndexFuncResult result : indexFuncResults) {
      switch (result.getIndexFunc()) {
        case TIME_RANGE:
        case SIM_ST:
        case SIM_ET:
        case SERIES_LEN:
        case ED:
        case DTW:
          result.setIsTensor(true);
          break;
        default:
          throw new UnsupportedIndexQueryException(indexFuncResults.toString());
      }
      result.setIndexFuncDataType(result.getIndexFunc().getType());
    }
    if (queryConditions.containsKey(THRESHOLD)) {
      this.threshold = Double.parseDouble(queryConditions.get(THRESHOLD));
    } else {
      this.threshold = Double.MAX_VALUE;
    }
    if (queryConditions.containsKey(PATTERN)) {
      this.patterns = IndexUtils.parseNumericPattern(queryConditions.get(PATTERN));
    } else {
      throw new UnsupportedIndexQueryException("missing parameter: " + PATTERN);
    }
  }

  @Override
  protected BiConsumer<Integer, ByteBuffer> getDeserializeFunc() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected List<Identifier> getQueryCandidates(List<Integer> candidateIds) {

    throw new UnsupportedOperationException();
  }

  @Override
  protected void calcAndFillQueryFeature() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected double calcLowerBoundThreshold(double queryThreshold) {
    return 0;
  }

  @Override
  public int postProcessNext(List<IndexFuncResult> funcResult) throws IndexQueryException {
    TVList aligned = (TVList) indexPreprocessor.getCurrent_L2_AlignedSequence();
    double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
    System.out.println(String.format(
        "ELB Process: ed:%.3f: %s", ed, IndexUtils.tvListToStr(aligned)));
    int reminding = funcResult.size();
    if (ed <= threshold) {
      for (IndexFuncResult result : funcResult) {
        IndexFuncFactory.basicSimilarityCalc(result, indexPreprocessor, patterns);
      }
    }
    TVListAllocator.getInstance().release(aligned);
    return reminding;
  }
}
