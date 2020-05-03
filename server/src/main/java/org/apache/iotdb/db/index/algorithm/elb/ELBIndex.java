package org.apache.iotdb.db.index.algorithm.elb;

import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_ELB_CALC_PARAM;
import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_CALC_PARAM;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_CALC_PARAM_SINGLE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_DEFAULT_THRESHOLD_RATIO;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_THRESHOLD_BASE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_THRESHOLD_RATIO;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.iotdb.db.index.algorithm.MBRIndex;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.pattern.CalcParam;
import org.apache.iotdb.db.index.algorithm.elb.pattern.SingleParamSchema;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Use PAA as the feature of MBRIndex.</p>
 */

public class ELBIndex extends MBRIndex {

  private static final Logger logger = LoggerFactory.getLogger(ELBIndex.class);
  private Distance distance;
  private ELBType elbType;
  private CalcParam calcParam;


  private ELBCountFixedPreprocessor elbTimeFixedPreprocessor;

  public ELBIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo, true);
    initELBParam();
  }

  private void initELBParam() {
    this.distance = Distance.getDistance(props.getOrDefault(DISTANCE, DEFAULT_DISTANCE));
    elbType = ELBType.valueOf(props.getOrDefault(ELB_TYPE, DEFAULT_ELB_TYPE));
    String calcParamName = props.getOrDefault(ELB_CALC_PARAM, DEFAULT_ELB_CALC_PARAM);
    if (ELB_CALC_PARAM_SINGLE.equals(calcParamName)) {
      double thresholdBase = Double.parseDouble(props.getOrDefault(ELB_THRESHOLD_BASE, "-1"));
      double thresholdRatio = Double.parseDouble(props.getOrDefault(ELB_THRESHOLD_RATIO, "-1"));
      if (thresholdBase == -1 && thresholdRatio == -1) {
        thresholdRatio = ELB_DEFAULT_THRESHOLD_RATIO;
      }
      this.calcParam = new SingleParamSchema(thresholdBase, thresholdRatio, windowRange);
    }
  }

  @Override
  public IndexPreprocessor initIndexPreprocessor(TVList tvList) {
    if (this.indexPreprocessor != null) {
      this.indexPreprocessor.clear();
    }
    this.elbTimeFixedPreprocessor = new ELBCountFixedPreprocessor(tvList, windowRange, slideStep,
        featureDim, distance, calcParam, elbType, true, false, false);
    this.indexPreprocessor = elbTimeFixedPreprocessor;
    return indexPreprocessor;
  }

  /**
   * Fill {@code currentCorners} and the optional {@code currentRanges}, and return the current idx
   *
   * @return the current idx
   */
  @Override
  protected int fillCurrentFeature() {
    elbTimeFixedPreprocessor.copyFeature(currentCorners, currentRanges);
    return elbTimeFixedPreprocessor.getCurrentIdx();
  }

  @Override
  protected BiConsumer<Integer, OutputStream> getSerializeFunc() {
    return (idx, outputStream) -> {
      try {
        elbTimeFixedPreprocessor.serializeIdentifier(idx, outputStream);
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
