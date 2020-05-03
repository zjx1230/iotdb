package org.apache.iotdb.db.index.io;

import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXING_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_MAGIC;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;


public class IndexIOWriter {

  private final TsFileOutput output;
  private final Map<String, Map<IndexType, List<IndexChunkMeta>>> metaDataMap;
  private final String indexFileName;
  private boolean closed;

  public IndexIOWriter(String indexFileName) {
    this.output = FSFactoryProducer.getFileOutputFactory()
        .getTsFileOutput(indexFileName + INDEXING_SUFFIX, false);
    this.indexFileName = indexFileName;
    this.metaDataMap = new HashMap<>();
    this.closed = false;
  }

  /**
   * thread-safety should be ensured by caller. The buffer length will be saved in ChunkInfo and
   * serialized into the second-layer metadata.
   *
   * @param flushChunk contains information of a flushing chunk
   */
  public void writeIndexData(IndexFlushChunk flushChunk) throws IOException {
    Map<IndexType, List<IndexChunkMeta>> indexTypeListMap = metaDataMap
        .putIfAbsent(flushChunk.path, new EnumMap<>(IndexType.class));
    if (indexTypeListMap == null) {
      indexTypeListMap = metaDataMap.get(flushChunk.path);
    }
    List<IndexChunkMeta> chunkInfoList = indexTypeListMap
        .putIfAbsent(flushChunk.indexType, new ArrayList<>());
    if (chunkInfoList == null) {
      chunkInfoList = indexTypeListMap.get(flushChunk.indexType);
    }
    IndexChunkMeta chunkInfo = new IndexChunkMeta(flushChunk.startTime, flushChunk.endTime,
        output.getPosition(), flushChunk.getDataSize());
    chunkInfoList.add(chunkInfo);
    flushChunk.data.writeTo(output.wrapAsStream());
  }

  public void endFile() throws IOException {
    if (closed) {
      return;
    }
    OutputStream outputStream = output.wrapAsStream();

    PublicBAOS firstLayerMeta = new PublicBAOS();
    ReadWriteIOUtils.write(metaDataMap.size(), firstLayerMeta);
    // The second layer metadata, only the chunkInfos of all index data bytes
    for (Entry<String, Map<IndexType, List<IndexChunkMeta>>> pathEntry : metaDataMap.entrySet()) {
      String path = pathEntry.getKey();
      Map<IndexType, List<IndexChunkMeta>> indexChunkListMap = pathEntry.getValue();
      ReadWriteIOUtils.write(path, firstLayerMeta);
      ReadWriteIOUtils.write(indexChunkListMap.size(), firstLayerMeta);
      for (Entry<IndexType, List<IndexChunkMeta>> indexTypeEntry : indexChunkListMap.entrySet()) {
        IndexType indexType = indexTypeEntry.getKey();
        List<IndexChunkMeta> chunkList = indexTypeEntry.getValue();
        ReadWriteIOUtils.write(indexType.serialize(), firstLayerMeta);
        ReadWriteIOUtils.write(output.getPosition(), firstLayerMeta);
        ReadWriteIOUtils.write(chunkList.size(), outputStream);
        for (IndexChunkMeta chunkInfo : chunkList) {
          chunkInfo.serializeTo(outputStream);
        }
      }
    }
    // Output the first layer metadata. A map of path-index_type-chunk_info_link,
    // where chunk_info_pos point to the serialized chunk info metadata.
    long firstLayerPosition = output.getPosition();
    firstLayerMeta.writeTo(outputStream);
    ReadWriteIOUtils.write((int) (output.getPosition() - firstLayerPosition), outputStream);
    // file ends with the magic bytes as the check code.
    output.write(INDEX_MAGIC.getBytes());
    output.close();

    //indexing file is renamed to indexed file,
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    File src = fsFactory.getFile(indexFileName + INDEXING_SUFFIX);
    File dest = fsFactory.getFile(indexFileName + INDEXED_SUFFIX);
    dest.delete();
    fsFactory.moveFile(src, dest);
    closed = true;
  }

  /**
   * IoTDBIndex builds indexes for sequences within a time range of one path. This IndexChunkInfo
   * stores the earliest and the latest timestamp of the sequence, and the position in the index
   * file.
   */
  public static class IndexChunkMeta {

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

  }


  public static class IndexFlushChunk {

    final String path;
    final IndexType indexType;
    final ByteArrayOutputStream data;
    final long startTime;
    final long endTime;

    public IndexFlushChunk(String path, IndexType indexType, ByteArrayOutputStream data,
        long startTime, long endTime) {
      this.path = path;
      this.indexType = indexType;
      this.data = data;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public int getDataSize() {
      return data.size();
    }

    @Override
    public String toString() {
      return String.format("IndexFlushChunk{path=%s, indexType=%s, data=[len=%d], "
          + "startTime=%d, endTime=%d}", path, indexType, data.size(), startTime, endTime);
    }
  }
}
