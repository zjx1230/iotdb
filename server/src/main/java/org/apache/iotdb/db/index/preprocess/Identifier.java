package org.apache.iotdb.db.index.preprocess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Calling functions {@linkplain IndexPreprocessor#getCurrent_L1_Identifier()
 * getLatestN_L1_Identifiers} and {@linkplain IndexPreprocessor#getLatestN_L1_Identifiers(int)
 * getLatestN_L1_Identifiers} will create {@code Identifier} object, which will bring additional
 * cost. Currently we adopt this simple interface definition. If {@code L1_Identifier} is called
 * frequently in the future, we will optimize it with cache or other methods.
 */
public class Identifier {

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public int getSubsequenceLength() {
    return subsequenceLength;
  }

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

  public void serialize(OutputStream output) throws IOException {
    ReadWriteIOUtils.write(startTime, output);
    ReadWriteIOUtils.write(endTime, output);
    ReadWriteIOUtils.write(subsequenceLength, output);
  }

  public static Identifier deserialize(ByteBuffer input) {
    long st = ReadWriteIOUtils.readLong(input);
    long ed = ReadWriteIOUtils.readLong(input);
    int length = ReadWriteIOUtils.readInt(input);
    return new Identifier(st, ed, length);
  }
}
