package org.apache.iotdb.db.index.algorithm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.CountFixedPreprocessor;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.read.IndexFuncResult;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NoIndex do nothing on feature extracting and data pruning. Its index-available range is always
 * empty.
 */

public class NoIndex extends IoTDBIndex {

  private static final Logger logger = LoggerFactory.getLogger(NoIndex.class);

  public NoIndex(String path, IndexInfo indexInfo) {
    super(path, indexInfo);
  }

  @Override
  public void initPreprocessor(ByteBuffer previous) {
    if (this.indexPreprocessor != null) {
      this.indexPreprocessor.clear();
    }
    this.indexPreprocessor = new CountFixedPreprocessor(tsDataType, windowRange,
        slideStep, true, true);
    indexPreprocessor.deserializePrevious(previous);
  }

  @Override
  public boolean buildNext() {
    return true;
  }

  /**
   * convert the L1 identifiers to byteArray
   */
  @Override
  public IndexFlushChunk flush() {
    if (indexPreprocessor.getCurrentChunkSize() == 0) {
      System.out.println(String.format("%s-%s not input why flush? return", path, indexType));
      return null;
    }
    List<Object> list = indexPreprocessor.getAll_L1_Identifiers();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(list.size());
    try {
      ReadWriteIOUtils.write(list.size(), baos);
      for (Object o : list) {
        Identifier id = (Identifier) o;
        id.serialize(baos);
      }
    } catch (IOException e) {
      logger.error("flush failed", e);
      return null;
    }
    long st = ((Identifier) list.get(0)).getStartTime();
    long end = ((Identifier) list.get(list.size() - 1)).getEndTime();
    return new IndexFlushChunk(path, indexType, baos, st, end);

  }

  /**
   * Nothing to be cleared, no more memory is released. Thus, we call the super method directly.
   * Just for explain.
   *
   * @return 0
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public long clear() {
    return super.clear();
  }


  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }

  /**
   * NoIndex cannot prune anything. Return null.
   */
  @Override
  public List<Identifier> queryByIndex(ByteBuffer indexChunkData) throws IndexManagerException {
    return null;
  }

  @Override
  public boolean postProcessNext(IndexFuncResult funcResult) throws IndexManagerException {
    throw new UnsupportedOperationException();
  }


  /**
   * All it needs depends on its preprocessor. Just for explain.
   */
  @Override
  @SuppressWarnings("squid:S1185")
  public int getAmortizedSize() {
    return super.getAmortizedSize();
  }

}
