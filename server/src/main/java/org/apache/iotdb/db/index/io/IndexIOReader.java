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
import org.apache.iotdb.db.index.read.IndexFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * refer to {@linkplain TsFileSequenceReader} It's thread-unsafe.
 */
public class IndexIOReader {


  private final Map<String, Map<IndexType, IndexPair>> metaDataMap;
  private final String indexFileName;
  private InputStream indexInputStream;
  private TsFileInput indexInput;


  public IndexIOReader(String indexFileName, boolean lazyLoad) throws IOException {
    if (!indexFileName.endsWith(INDEXED_SUFFIX)) {
      indexFileName += INDEXED_SUFFIX;
    }
    this.indexFileName = indexFileName;
    metaDataMap = loadFirstMetadata();
    // If not lazyLoad, load all metadata immediately, otherwise, load metadata when required.
    if (!lazyLoad) {
      loadAllSecondMetadata();
    }
  }


  /**
   * Init reader with generated metadataMap from {@linkplain IndexIOWriter}.
   *
   * @param metaDataMap generated metadataMap
   */
  public IndexIOReader(Map<String, Map<IndexType, IndexPair>> metaDataMap, String indexFileName,
      boolean lazyLoad) throws IOException {
    if (!indexFileName.endsWith(INDEXED_SUFFIX)) {
      indexFileName += INDEXED_SUFFIX;
    }
    this.indexFileName = indexFileName;
    this.metaDataMap = metaDataMap;
    if (!lazyLoad) {
      loadAllSecondMetadata();
    }
  }


  private void checkIOReader() throws IOException {
    if (indexInput == null) {
      this.indexInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(indexFileName);
      this.indexInputStream = indexInput.wrapAsInputStream();
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
    checkIOReader();
    indexInput.position(startPos);
    int size = ReadWriteIOUtils.readInt(indexInputStream);
    ArrayList<IndexChunkMeta> res = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
//      getDataByChunkMeta

      IndexChunkMeta chunkMeta = IndexChunkMeta.deserializeFrom(indexInputStream);
      res.add(chunkMeta);
    }
    return res;
  }

  @FunctionalInterface
  public static interface ReadDataByChunkMetaCallback {

    ByteBuffer call(IndexChunkMeta indexMeta) throws IOException;
  }

  /**
   * read the data bytes from file according to the position in indexMeta
   */
  public ByteBuffer getDataByChunkMeta(IndexChunkMeta indexMeta) throws IOException {
    checkIOReader();
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
    pair.list.forEach(p -> p.setReadDataCallback(this::getDataByChunkMeta));
    return Collections.unmodifiableList(pair.list);
  }

  /**
   * Load the first layer metadata.
   */
  private Map<String, Map<IndexType, IndexPair>> loadFirstMetadata() throws IOException {
    checkIOReader();
    int firstMetaSize = loadFirstMetadataSize();
    indexInput.position(firstMetaSize);
    ByteBuffer firstMetaBytes = ByteBuffer.allocate(firstMetaSize);
    indexInput.read(firstMetaBytes, indexInput.size() - INDEX_MAGIC.getBytes().length
        - Integer.BYTES - firstMetaSize);
    firstMetaBytes.flip();
    // reconstruct
    int pathSize = ReadWriteIOUtils.readInt(firstMetaBytes);
    Map<String, Map<IndexType, IndexPair>> metaDataMap = new HashMap<>(pathSize);
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
    return metaDataMap;
  }

  private int loadFirstMetadataSize() throws IOException {
    if (checkTailComplete(this.indexInput)) {
      indexInput.position(indexInput.size() - INDEX_MAGIC.getBytes().length - Integer.BYTES);
      return ReadWriteIOUtils.readInt(indexInputStream);
    } else {
      return -1;
    }
  }

  /**
   * Used to load metadata and recover files.
   */
  public static boolean checkTailComplete(TsFileInput indexInput) throws IOException {
    return INDEX_MAGIC.equals(readTailIndexMagic(indexInput));
  }


  private static String readTailIndexMagic(TsFileInput indexInput) throws IOException {
    long totalSize = indexInput.size();
    if (totalSize < INDEX_MAGIC.getBytes().length) {
      return "";
    }
    ByteBuffer magicStringBytes = ByteBuffer.allocate(INDEX_MAGIC.getBytes().length);
    indexInput.read(magicStringBytes, totalSize - INDEX_MAGIC.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  public static class IndexPair {

    long chunkListPos;
    List<IndexChunkMeta> list;

    IndexPair(long chunkListPos) {
      this.chunkListPos = chunkListPos;
    }

    IndexPair(List<IndexChunkMeta> list) {
      this.list = list;
      this.chunkListPos = -1;
    }
  }

  /**
   * Package-private, only for {@linkplain org.apache.iotdb.db.index.read.IndexFileResource
   * IndexFileResource} and test.
   */
  Map<String, Map<IndexType, IndexPair>> getMetaDataMap() {
    return metaDataMap;
  }

}
