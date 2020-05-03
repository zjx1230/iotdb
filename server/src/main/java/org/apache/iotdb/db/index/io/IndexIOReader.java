package org.apache.iotdb.db.index.io;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_MAGIC;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexChunkMeta;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * refer to {@linkplain TsFileSequenceReader}
 */
public class IndexIOReader {

  private final TsFileInput indexInput;


  private Map<String, Map<IndexType, IndexPair>> metaDataMap;
  private final InputStream indexInputStream;

  public IndexIOReader(String indexFileName, boolean lazyLoad) throws IOException {
    if (!indexFileName.endsWith(INDEXED_SUFFIX)) {
      indexFileName += INDEXED_SUFFIX;
    }
    this.indexInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(indexFileName);
    this.indexInputStream = indexInput.wrapAsInputStream();
    loadFirstMetadata();
    // If not lazyLoad, load all metadata immediately, otherwise, load metadata when required.
    if (!lazyLoad) {
      loadAllSecondMetadata();
    }
  }

  private void loadAllSecondMetadata() throws IOException {
    for (Entry<String, Map<IndexType, IndexPair>> pathEntry : metaDataMap.entrySet()) {
      Map<IndexType, IndexPair> indexChunkListMap = pathEntry.getValue();
      for (Entry<IndexType, IndexPair> indexTypeEntry : indexChunkListMap.entrySet()) {
        IndexPair pair = indexTypeEntry.getValue();
        pair.list = loadOnePathMetadata(pair.chunkListPos);
      }
    }
  }


  private List<IndexChunkMeta> loadOnePathMetadata(long startPos) throws IOException {
    indexInput.position(startPos);
    int size = ReadWriteIOUtils.readInt(indexInputStream);
    ArrayList<IndexChunkMeta> res = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      res.add(IndexChunkMeta.deserializeFrom(indexInputStream));
    }
    return res;
  }

  /**
   * read the data bytes from file according to the position in indexMeta
   */
  public ByteBuffer getDataByChunkMeta(IndexChunkMeta indexMeta) throws IOException {
    long startPos = indexMeta.startPosInFile;
    int dataSize = indexMeta.dataSize;
    ByteBuffer indexBytes = ByteBuffer.allocate(dataSize);
    indexInput.read(indexBytes, startPos);
    indexBytes.flip();
    return indexBytes;
  }

  /**
   * load and return the index chunk metadata for the given path.
   *
   * @param path given path.
   * @param indexType specified path.
   * @return the returned list is unmodifiable.
   */
  public List<IndexChunkMeta> getChunkMetas(String path, IndexType indexType)
      throws IOException {
    if (metaDataMap == null) {
      loadFirstMetadata();
    }
    if (metaDataMap == null) {
      throw new IOException("load first layer metadata failed");
    }
    if (!metaDataMap.containsKey(path) || !metaDataMap.get(path).containsKey(indexType)) {
      return new ArrayList<>();
    }
    IndexPair pair = metaDataMap.get(path).get(indexType);
    // lazy mode, not initialized
    if (pair.list == null) {
      pair.list = loadOnePathMetadata(pair.chunkListPos);
    }
    return Collections.unmodifiableList(pair.list);
  }

  /**
   * Load the first layer metadata.
   */
  private void loadFirstMetadata() throws IOException {
    int firstMetaSize = loadFirstMetadataSize();
    indexInput.position(firstMetaSize);
    ByteBuffer firstMetaBytes = ByteBuffer.allocate(firstMetaSize);
    indexInput.read(firstMetaBytes, indexInput.size() - INDEX_MAGIC.getBytes().length
        - Integer.BYTES - firstMetaSize);
    firstMetaBytes.flip();
    // reconstruct
    int pathSize = ReadWriteIOUtils.readInt(firstMetaBytes);
    metaDataMap = new HashMap<>(pathSize);
    for (int i = 0; i < pathSize; i++) {
      String path = ReadWriteIOUtils.readString(firstMetaBytes);
      int indexSize = ReadWriteIOUtils.readInt(firstMetaBytes);
      Map<IndexType, IndexPair> indexChunkPairMap = new EnumMap<>(IndexType.class);
      for (int j = 0; j < indexSize; j++) {
        IndexType indexType = IndexType.deserialize(ReadWriteIOUtils.readShort(firstMetaBytes));
        long chunkPos = ReadWriteIOUtils.readLong(firstMetaBytes);
        indexChunkPairMap.put(indexType, new IndexPair(chunkPos));
      }
      metaDataMap.put(path, indexChunkPairMap);
    }
  }


  private int loadFirstMetadataSize() throws IOException {
    if (readTailIndexMagic().equals(INDEX_MAGIC)) {
      indexInput.position(indexInput.size() - INDEX_MAGIC.getBytes().length - Integer.BYTES);
      return ReadWriteIOUtils.readInt(indexInputStream);
    } else {
      return -1;
    }
  }

  private String readTailIndexMagic() throws IOException {
    long totalSize = indexInput.size();
    ByteBuffer magicStringBytes = ByteBuffer.allocate(INDEX_MAGIC.getBytes().length);
    indexInput.read(magicStringBytes, totalSize - INDEX_MAGIC.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  static class IndexPair {

    long chunkListPos;
    List<IndexChunkMeta> list;

    IndexPair(long chunkListPos) {
      this.chunkListPos = chunkListPos;
    }
  }

  /**
   * Package-private, only for test.
   */
  Map<String, Map<IndexType, IndexPair>> getMetaDataMap() {
    return metaDataMap;
  }

}
