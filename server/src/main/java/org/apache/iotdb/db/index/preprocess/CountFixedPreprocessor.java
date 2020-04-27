package org.apache.iotdb.db.index.preprocess;

/**
 * COUNT_FIXED is very popular in the preprocessing phase. Most previous researches build indexes on
 * sequences with the same length (a.k.a. COUNT_FIXED). The timestamp is very important for time
 * series, although a rich line of researches ignore the time information directly.<p>
 *
 * {@code WindowRange}: the number of subsequence.<p>
 *
 * {@code SlideStep}: the offset between two adjacent subsequences<p>
 *
 * {@code needFill}: inactive for COUNT_FIXED<p>
 *
 * Note that, in real scenarios, the time series could be variable-frequency, traditional distance
 * metrics (e.g., Euclidean distance) may not make sense for COUNT_FIXED.
 */
public class CountFixedPreprocessor extends BasicPreprocessor {

  /**
   * We don't put these two fields into {@linkplain BasicPreprocessor}, because we think that
   * although the three-layer features are pre-designed, {@linkplain BasicPreprocessor} doesn't
   * maintain any fields about them. It's weird to control some fields you haven't even seen.
   */
  protected final boolean storeIdentifier;
  protected final boolean storeAligned;

  public CountFixedPreprocessor(int windowRange, int slideStep, boolean storeIdentifier,
      boolean storeAligned) {
    super(WindowType.COUNT_FIXED, windowRange, slideStep);
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
  }

  public CountFixedPreprocessor(int subSeqLength, int slideStep) {
    this(subSeqLength, slideStep, true, true);
  }

  public CountFixedPreprocessor(int subSeqLength) {
    this(subSeqLength, 1, true, true);
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void processNext() {

  }

  @Override
  public Object getPastN_L1_Identifiers(int pastN) {
    return null;
  }

  @Override
  public Object getPastN_L2_AlignedSequences(int pastN) {
    return null;
  }
}
