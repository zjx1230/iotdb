package org.apache.iotdb.db.index.preprocess;

import org.apache.iotdb.tsfile.exception.NotImplementedException;

/**
 * For all indexes, the raw input sequence has to be pre-processed before it's organized by indexes.
 * In general, index structure needn't maintain all of original data, but only pointers to the
 * original data (e.g. The start time and the end time can uniquely determine a time sequence).<p>
 *
 * {@linkplain BasicPreprocessor} makes a time window slide over the time series by some rules and
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
public abstract class BasicPreprocessor {

  /**
   * the type of sliding window, see {@linkplain WindowType}
   */
  protected WindowType widthType;
  /**
   * the window range, it depends on the window type.
   */
  protected int windowRange;
  /**
   * the slide step (or called the update size), it depends on the window type.
   */
  protected int slideStep;


  public BasicPreprocessor(WindowType widthType, int windowRange, int slideStep) {
    this.windowRange = windowRange;
    this.widthType = widthType;
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
   * get the past N of L1 identifiers.
   */
  public abstract Object getPastN_L1_Identifiers(int pastN);

  /**
   * get the last L1.
   */
  public Object getLast_L1_Identifier() {
    return getPastN_L1_Identifiers(1);
  }


  /**
   * get the past N of L2 aligned sequences.
   */
  public abstract Object getPastN_L2_AlignedSequences(int pastN);

  /**
   * get the last L2 aligned sequences.
   */
  public Object getLast_L2_AlignedSequence() {
    return getPastN_L2_AlignedSequences(1);
  }

  /**
   * get the past N of L3 Features.
   *
   * @throws NotImplementedException Not all preprocessors support L3 features.
   */
  public Object getPastN_L3_Features(int pastN) {
    throw new NotImplementedException("This preprocessor doesn't support L3 feature");
  }

  /**
   * get the last L3 Features.
   *
   * @throws NotImplementedException Not all preprocessors support L3 features.
   */
  public Object getLast_L3_Feature() {
    return getPastN_L3_Features(1);
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
}
