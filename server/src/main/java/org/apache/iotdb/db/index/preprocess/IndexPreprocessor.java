package org.apache.iotdb.db.index.preprocess;

import java.util.List;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

/**
 * For all indexes, the raw input sequence has to be pre-processed before it's organized by indexes.
 * In general, index structure needn't maintain all of original data, but only pointers to the
 * original data (e.g. The start time and the end time can uniquely determine a time sequence).<p>
 *
 * {@linkplain IndexPreprocessor} makes a time window slide over the time series by some rules and
 * obtain a list of subsequences. The time windows may be time-fixed (Euclidean distance),
 * count-fixed (Time Warping). It scans the sequence with a certain overlap step (a.k.a. the update
 * size).
 *
 * A time window may be aligned to equal interval or equal range, which is called "Aligned
 * Sequences."<p>
 *
 * Many indexes will further extract features of alignment sequences, such as PAA, SAX, FFT, etc.
 * <p>
 *
 * After preprocessing, the subsequence will have three-level features:
 * <ul>
 *   <li>L1: a triplet to identify a subsequence: {@code {StartTime, EndTime, Length}}</li>
 *   <li>L2: aligned sequence: {@code {a1, a2, ..., an}}</li>
 *   <li>L3: customized feature: {@code {C1, C2, ..., Cm}}</li>
 * </ul>
 */
public abstract class IndexPreprocessor {

  /**
   * the type of sliding window, see {@linkplain WindowType}
   */
  protected WindowType windowType;
  /**
   * the window range, it depends on the window type.
   */
  protected int windowRange;
  /**
   * the the offset between two adjacent subsequences (a.k.a. the update size), depending on the the
   * window type.
   */
  protected int slideStep;
  protected final TVList srcData;


  public IndexPreprocessor(TVList srcData, WindowType widthType, int windowRange, int slideStep) {
    this.srcData = srcData;
    this.windowRange = windowRange;
    this.windowType = widthType;
    this.slideStep = slideStep;
  }

  /**
   * Returns true if the pre-processor has more elements. i.e., return true if {@link #processNext}
   * would process the next data item rather than throwing an exception.)
   *
   * @return {@code true} if there are more elements to be processed.
   */
  public abstract boolean hasNext();

  /**
   * Processed the next element.
   */
  public abstract void processNext();

  /**
   * TVList may have been flushed many times, return the current offset.
   * @return the current offset.
   */
  public abstract int getCurrentChunkOffset();

  /**
   * In this chunk, how many points have been processed.
   * @return
   */
  public abstract int getCurrentChunkSize();

  /**
   * get the latest N L1-identifiers, including the current one. The caller needs to release them
   * after use.
   */
  public abstract List<Object> getLatestN_L1_Identifiers(int latestN);

  /**
   * get current L1 identifier. The caller needs to release them after use.
   */
  public Object getCurrent_L1_Identifier() {
    List<Object> res = getLatestN_L1_Identifiers(1);
    return res.isEmpty() ? null : res.get(0);
  }

  public List<Object> getAll_L1_Identifiers() {
    int chunkNum = getCurrentChunkSize();
    return getLatestN_L1_Identifiers(chunkNum);
  }

  /**
   * get the latest N of L2 aligned sequences, including the current one. The caller needs to
   * release them after use.
   */
  public abstract List<Object> getLatestN_L2_AlignedSequences(int latestN);

  /**
   * get current L2 aligned sequences. The caller needs to release them after use.
   */
  public Object getCurrent_L2_AlignedSequence() {
    List<Object> res = getLatestN_L2_AlignedSequences(1);
    return res.isEmpty() ? null : res.get(0);
  }

  /**
   * get the latest N of L3 Features.
   *
   * @throws NotImplementedException Not all preprocessors support L3 features.
   */
  public List<Object> getLatestN_L3_Features(int latestN) {
    throw new NotImplementedException("This preprocessor doesn't support L3 feature");
  }

  /**
   * get the current L3 Features.
   *
   * @throws NotImplementedException Not all preprocessors support L3 features.
   */
  public Object getCurrent_L3_Feature() {
    List<Object> res = getLatestN_L3_Features(1);
    return res.isEmpty() ? null : res.get(0);
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public int getWindowRange() {
    return windowRange;
  }

  public int getSlideStep() {
    return slideStep;
  }

  /**
   * The time window type: time-fixed or count-fixed. It determines the meanings of following
   * fields: {@code windowRange}, {@code windowRange}, {@code slideStep}.<p>
   *
   * COUNT_FIXED: TIME_FIXED:
   *
   * <p>See also: Kanat et. al. General Incremental Sliding-Window Aggregation. VLDB 2015.
   */
  public static enum WindowType {
    TIME_FIXED, COUNT_FIXED;

    public static WindowType getIndexType(String windowType) {
      String normalized = windowType.toUpperCase();
      switch (normalized) {
        case "TIME_FIXED":
          return TIME_FIXED;
        case "COUNT_FIXED":
          return COUNT_FIXED;
        default:
          throw new NotImplementedException("unsupported window type:" + windowType);
      }
    }
  }

  /**
   * Called when the memory reaches the threshold. This function should release all allocated array
   * list which increases with the number of processed data pairs.<p>
   *
   * Note that, after cleaning up all past store, the next {@linkplain #processNext()} will still
   * start from the current point.
   */
  public abstract long clear();


  /**
   * return how much memory is increased for each point processed. It's an amortized estimation,
   * depending on {@code storeIdentifier}, {@code storeAlignedSequence} and {@code storeFeature}
   */
  public abstract int getAmortizedSize();
}
