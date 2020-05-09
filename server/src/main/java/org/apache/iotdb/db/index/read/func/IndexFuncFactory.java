package org.apache.iotdb.db.index.read.func;

import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.db.index.common.IndexRuntimeException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;

public class IndexFuncFactory {


  public static double calcEuclidean(TVList aligned, double[] patterns) {
    if (aligned.size() != patterns.length) {
      throw new IndexRuntimeException("Sliding windows and modes are not equal in length");
    }
    double ed = 0;
    for (int i = 0; i < patterns.length; i++) {
      double d = IndexUtils.getDoubleFromAnyType(aligned, i) - patterns[i];
      ed += d * d;
    }
    return Math.sqrt(ed);
  }

  public static double calcDTW(TVList aligned, double[] patterns) {
    throw new UnsupportedOperationException("It's easy, but not written yet");
  }

  public static void basicSimilarityCalc(IndexFuncResult funcResult,
      IndexPreprocessor indexPreprocessor, double[] patterns)
      throws IndexQueryException {
    Identifier identifier;
    TVList aligned;
    switch (funcResult.getIndexFunc()) {
      case TIME_RANGE:
        identifier = indexPreprocessor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getEndTime() - identifier.getStartTime());
        break;
      case SERIES_LEN:
        identifier = indexPreprocessor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getSubsequenceLength());
        break;
      case SIM_ST:
        identifier = indexPreprocessor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getStartTime());
        break;
      case SIM_ET:
        identifier = indexPreprocessor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getEndTime());
        break;
      case ED:
        aligned = (TVList) indexPreprocessor.getCurrent_L2_AlignedSequence();
        double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
        funcResult.addScalar(ed);
        TVListAllocator.getInstance().release(aligned);
        break;
      case DTW:
        aligned = (TVList) indexPreprocessor.getCurrent_L2_AlignedSequence();
        double dtw = IndexFuncFactory.calcDTW(aligned, patterns);
        funcResult.addScalar(dtw);
        TVListAllocator.getInstance().release(aligned);
        break;
      default:
        throw new IndexQueryException("Unsupported query:" + funcResult.getIndexFunc());
    }
  }

}
