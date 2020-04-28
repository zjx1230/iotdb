package org.apache.iotdb.db.index.preprocess;

/**
 * Calling functions {@linkplain BasicPreprocessor#getCurrent_L1_Identifier()
 * getLatestN_L1_Identifiers} and {@linkplain BasicPreprocessor#getLatestN_L1_Identifiers(int)
 * getLatestN_L1_Identifiers} will create {@code Identifier} object, which will bring additional
 * cost. Currently we adopt this simple interface definition. If {@code L1_Identifier} is called
 * frequently in the future, we will optimize it with cache or other methods.
 */
public class Identifier {

  private long startTime;
  private long endTime;
  private int subsequenceLength;

  public Identifier(long startTime, long endTime, int subsequenceLength) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.subsequenceLength = subsequenceLength;
  }

  @Override
  public String toString() {
    return String.format("[%d-%d,%d]", startTime, endTime, subsequenceLength);
  }
}
