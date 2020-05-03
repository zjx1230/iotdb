package org.apache.iotdb.db.index.algorithm.paa;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.algorithm.MBRIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
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
  public IndexPreprocessor initIndexPreprocessor(TVList tvList) {
    if (this.indexPreprocessor != null) {
      this.indexPreprocessor.clear();
    }
    this.paaTimeFixedPreprocessor = new PAATimeFixedPreprocessor(tvList, windowRange, slideStep,
        featureDim, true, false);
    this.indexPreprocessor = paaTimeFixedPreprocessor;
    return indexPreprocessor;
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
    return paaTimeFixedPreprocessor.getCurrentIdx();
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
  public Object queryByIndex(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals, int limitSize) throws IndexManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object queryByScan(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals, int limitSize) throws IndexManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }
}
