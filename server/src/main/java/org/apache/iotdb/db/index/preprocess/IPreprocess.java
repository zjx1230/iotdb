package org.apache.iotdb.db.index.preprocess;

public interface IPreprocess {

  /**
   * Returns true if the pre-processor has more elements. i.e., return true if {@link #processNext}
   * would process the next data item rather than throwing an exception.)
   *
   * @return {@code true} if there are more elements to be processed.
   */
  boolean hasNext();

  /**
   * Processed the next element.
   */
  void processNext();

}
