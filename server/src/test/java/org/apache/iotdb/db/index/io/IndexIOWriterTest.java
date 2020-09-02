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

import static org.apache.iotdb.db.index.IndexTestUtils.TEST_INDEX_FILE_NAME;
import static org.apache.iotdb.db.index.common.IndexType.ELB;
import static org.apache.iotdb.db.index.common.IndexType.PAA_INDEX;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.IndexTestUtils;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexIOReader.IndexPair;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * refer to {@code RestorableTsFileIOWriterTest}
 */
public class IndexIOWriterTest {


  @Before
  public void setUp() throws Exception {
    IndexTestUtils.clearIndexFile(TEST_INDEX_FILE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    IndexTestUtils.clearIndexFile(TEST_INDEX_FILE_NAME);

  }


  @Test
  public void testBasicInput() throws IOException {
    TsFileOutput tsFileOutput = FSFactoryProducer
        .getFileOutputFactory().getTsFileOutput(TEST_INDEX_FILE_NAME, false);
    OutputStream outputStream = tsFileOutput.wrapAsStream();
    ReadWriteIOUtils.write(1L, outputStream);
    System.out.println(tsFileOutput.getPosition());
    ReadWriteIOUtils.write(1.2f, outputStream);
    System.out.println(tsFileOutput.getPosition());
    tsFileOutput.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
    System.out.println(tsFileOutput.getPosition());
    ReadWriteIOUtils.write(1.05d, outputStream);
    System.out.println(tsFileOutput.getPosition());
    tsFileOutput.close();
    TsFileInput tsFileInput = FSFactoryProducer
        .getFileInputFactory().getTsFileInput(TEST_INDEX_FILE_NAME);

    InputStream inputStream = tsFileInput.wrapAsInputStream();
    tsFileInput.position(8);
    System.out.println(ReadWriteIOUtils.readFloat(inputStream));
    ByteBuffer bs = ByteBuffer.allocate(4);
    tsFileInput.read(bs);
    System.out.println(Arrays.toString(bs.array()));
    System.out.println(ReadWriteIOUtils.readDouble(inputStream));
    ;
    tsFileInput.position(0);
    System.out.println(ReadWriteIOUtils.readLong(inputStream));
    ;

  }

  private void fakeInsert(String path, IndexType indexType, long st, long end, byte[] chunkData,
      IndexIOWriter writer, Map<String, Map<IndexType, List<IndexFlushChunk>>> dataMap)
      throws IOException {
    // add to ground truth
    Map<IndexType, List<IndexFlushChunk>> pathMap = dataMap.putIfAbsent(path,
        new EnumMap<>(IndexType.class));
    if (pathMap == null) {
      pathMap = dataMap.get(path);
    }
    List<IndexFlushChunk> indexList = pathMap.putIfAbsent(indexType, new ArrayList<>());
    if (indexList == null) {
      indexList = pathMap.get(indexType);
    }
    indexList.add(new IndexFlushChunk(path, indexType, toByteArray(chunkData), st, end));
    // insert into writer
    writer.writeIndexData(new IndexFlushChunk(path, indexType, toByteArray(chunkData), st, end));
  }

  private ByteArrayOutputStream toByteArray(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
    out.write(bytes, 0, bytes.length);
    return out;
  }

  @Test
  public void testIndexInputAndOutput() throws IOException {

//    Assert.assertArrayEquals(new byte[]{1, 2, 3}, new byte[]{1, 2, 3});
    // create fake data
//    Map<Integer, String> a = new HashMap<>();
//    System.out.println(a.putIfAbsent(1, "1"));
//    System.out.println(a.putIfAbsent(1, "1"));
    String p1 = "p1";
    String p2 = "p2";
    Map<String, Map<IndexType, List<IndexFlushChunk>>> gtMap = new HashMap<>();

    // writer
    IndexIOWriter writer = new IndexIOWriter(TEST_INDEX_FILE_NAME);
    fakeInsert(p1, ELB, 1, 5, new byte[]{1, 2, 3}, writer, gtMap);
    fakeInsert(p1, PAA_INDEX, 2, 7, new byte[]{11, 12, 13}, writer, gtMap);
    fakeInsert(p2, ELB, 3, 6, new byte[]{21, 22, 23}, writer, gtMap);
    fakeInsert(p1, PAA_INDEX, 10, 20, new byte[]{31, 32, 33}, writer, gtMap);
    writer.endFile();

    IndexIOReader reader = new IndexIOReader(TEST_INDEX_FILE_NAME, false);

    checkReaderResult(gtMap, reader);
  }

  /**
   * first check metaMap in reader is as expected, then check the returned data
   */
  private void checkReaderResult(Map<String, Map<IndexType, List<IndexFlushChunk>>> gtMap,
      IndexIOReader reader) {
    Map<String, Map<IndexType, IndexPair>> meta = reader.getMetaDataMap();
    Assert.assertEquals(gtMap.size(), meta.size());
    gtMap.forEach((path, gtPathMap) -> {
      Assert.assertTrue(meta.containsKey(path));
      Map<IndexType, IndexPair> metaPathMap = meta.get(path);
      Assert.assertEquals(gtPathMap.size(), metaPathMap.size());
      gtPathMap.forEach((indexType, gtChunkList) -> {
        Assert.assertTrue(metaPathMap.containsKey(indexType));

        try {
          List<IndexChunkMeta> metaChunkList = reader.getChunkMetas(path, indexType);
          Assert.assertEquals(gtChunkList.size(), metaChunkList.size());
          for (int i = 0; i < gtChunkList.size(); i++) {
            IndexChunkMeta chunkMeta = metaChunkList.get(i);
            IndexFlushChunk gtChunk = gtChunkList.get(i);
            Assert.assertEquals(chunkMeta.startTime, gtChunk.startTime);
            Assert.assertEquals(chunkMeta.endTime, gtChunk.endTime);
            // data
            ByteArrayOutputStream gtData = gtChunk.data;
            ByteBuffer readData = reader.getDataByChunkMeta(chunkMeta);
            Assert.assertArrayEquals(gtData.toByteArray(), readData.array());
          }
        } catch (IOException e) {
          Assert.fail(String.format("path %s, type %s: %s:", path, indexType, e));
        }
      });
    });
  }
}