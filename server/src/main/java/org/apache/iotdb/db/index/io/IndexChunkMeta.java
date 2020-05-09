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
package org.apache.iotdb.db.index.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iotdb.db.index.io.IndexIOReader.ReadDataByChunkMetaCallback;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class IndexChunkMeta {

  /**
   * IoTDBIndex builds indexes for sequences within a time range of one path. This IndexChunkInfo
   * stores the earliest and the latest timestamp of the sequence, and the position in the index
   * file.
   */

  public long getStartTime() {
    return startTime;
  }

  /**
   *
   */
  long startTime;

  public long getEndTime() {
    return endTime;
  }

  long endTime;


  /**
   * the position of index data in the file
   */
  long startPosInFile;
  int dataSize;
  private ReadDataByChunkMetaCallback getDataByChunkMeta;


  public IndexChunkMeta(long startTime, long endTime, long startPosInFile, int dataSize) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.startPosInFile = startPosInFile;
    this.dataSize = dataSize;
  }

  public ByteBuffer unpack() throws IOException {
    return getDataByChunkMeta.call(this);
  }


  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(startTime, outputStream);
    ReadWriteIOUtils.write(endTime, outputStream);
    ReadWriteIOUtils.write(startPosInFile, outputStream);
    ReadWriteIOUtils.write(dataSize, outputStream);
  }

  public static IndexChunkMeta deserializeFrom(InputStream inputStream) throws IOException {
    long startTime = ReadWriteIOUtils.readLong(inputStream);
    long endTime = ReadWriteIOUtils.readLong(inputStream);
    long startPosInFile = ReadWriteIOUtils.readLong(inputStream);
    int dataSize = ReadWriteIOUtils.readInt(inputStream);
    return new IndexChunkMeta(startTime, endTime, startPosInFile, dataSize);
  }

  void setReadDataCallback(ReadDataByChunkMetaCallback getDataByChunkMeta) {
    this.getDataByChunkMeta = getDataByChunkMeta;
  }

  public ByteBuffer getReadData() throws IOException {
    return getDataByChunkMeta == null ? null : getDataByChunkMeta.call(this);
  }

  @Override
  public String toString() {
    return "{" +
        "startTime=" + startTime +
        ", endTime=" + endTime +
        ", startPos=" + startPosInFile +
        ", dataSize=" + dataSize +
        '}';
  }

  public String toStringStable() {
    return "{" +
        "startTime=" + startTime +
        ", endTime=" + endTime +
        ", dataSize=" + dataSize +
        '}';
  }

}
