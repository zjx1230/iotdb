package org.apache.iotdb.db.index.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.index.io.IndexIOReader.ReadDataByChunkMetaCallback;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class IndexChunkMeta {

  /**
   * IoTDBIndex builds indexes for sequences within a time range of one path. This IndexChunkInfo
   * stores the earliest and the latest timestamp of the sequence, and the position in the index
   * file.
   */

  /**
   *
   */
  long startTime;
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
