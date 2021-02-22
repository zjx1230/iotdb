/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.preprocess;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Calling functions {@linkplain IndexFeatureExtractor#getCurrent_L1_Identifier()
 * getLatestN_L1_Identifiers} and {@linkplain IndexFeatureExtractor#getLatestN_L1_Identifiers(int)
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

  public static Identifier deserialize(InputStream input) throws IOException {
    long st = ReadWriteIOUtils.readLong(input);
    long ed = ReadWriteIOUtils.readLong(input);
    int length = ReadWriteIOUtils.readInt(input);
    return new Identifier(st, ed, length);
  }
}
