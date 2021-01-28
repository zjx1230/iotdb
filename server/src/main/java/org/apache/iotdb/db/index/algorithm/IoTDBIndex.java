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
package org.apache.iotdb.db.index.algorithm;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.DistSeries;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.IndexQueryDataSet;
import org.apache.iotdb.db.index.read.optimize.IIndexRefinePhaseOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * Each StorageGroupProcessor contains a IndexProcessor, and each IndexProcessor can contain more
 * than one IIndex. Each type of Index corresponds one IIndex.
 */
public abstract class IoTDBIndex {

  //  protected final String path;
  protected final PartialPath indexSeries;
  protected final IndexType indexType;
  protected final long confIndexStartTime;
  protected final Map<String, String> props;
  protected final TSDataType tsDataType;
  protected int windowRange;
  protected int slideStep;
  protected IndexFeatureExtractor indexFeatureExtractor;


  public IoTDBIndex(PartialPath indexSeries, TSDataType tsDataType, IndexInfo indexInfo) {
    this.indexSeries = indexSeries;
    this.indexType = indexInfo.getIndexType();
    this.confIndexStartTime = indexInfo.getTime();
    this.props = indexInfo.getProps();
    this.tsDataType = tsDataType;
    parsePropsAndInit(this.props);
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  private void parsePropsAndInit(Map<String, String> props) {
    // Strategy
    //WindowRange
    this.windowRange =
        props.containsKey(INDEX_WINDOW_RANGE) ? Integer.parseInt(props.get(INDEX_WINDOW_RANGE))
            : IoTDBDescriptor.getInstance().getConfig().getDefaultIndexWindowRange();
    // SlideRange
    this.slideStep = props.containsKey(INDEX_SLIDE_STEP) ?
        Integer.parseInt(props.get(INDEX_SLIDE_STEP)) : this.windowRange;
  }

  /**
   * Each index has its own preprocessor. Through the preprocessor provided by this index,
   * {@linkplain IndexProcessor IndexFileProcessor} can control the its data process, memory
   * occupation and triggers forceFlush.
   *
   * @param tvList tvList in current FlushTask.
   * @return Preprocessor with new data.
   */
  public IndexFeatureExtractor startFlushTask(PartialPath partialPath, TVList tvList) {
    this.indexFeatureExtractor.appendNewSrcData(tvList);
    return indexFeatureExtractor;
  }

//  /**
//   * Sorry but this method is ugly.
//   */
//  public IndexFeatureExtractor startFlushTask(BatchData batchData) {
//    this.indexFeatureExtractor.appendNewSrcData(batchData);
//    return indexFeatureExtractor;
//  }

  /**
   * An index should determine which preprocessor it uses and hook it to {@linkplain
   * IoTDBIndex}.indexProcessor. An index should determine which preprocessor it uses and connect to
   * the IndexProcessor.
   *
   * This method is called when IoTDBIndex is created and accept the previous overlapped data in
   * type of {@code ByteBuffer}.
   *
   * Note that, the implementation should call {@code deserializePrevious(ByteBuffer byteBuffer)}
   * after initialize the preprocessor.
   */
  public abstract void initPreprocessor(ByteBuffer previous, boolean inQueryMode);



  /**
   * When this function is called, it means that a new point has been pre-processed.  The index can
   * organize the newcomer in real time, or delay to build it until {@linkplain #flush}
   */
  public abstract boolean buildNext() throws IndexManagerException;

  /**
   * flush
   */
  public abstract void flush() throws IndexManagerException;

  /**
   * clear and release the occupied memory. The preprocessor has been cleared in IoTDBIndex, so
   * remember invoke {@code super.clear()} and then add yourself.
   *
   * This method is called when completing a sub-flush.  Note that one flush task will trigger
   * multiple sub-brush tasks due to the memory control.
   *
   * clear和其他的区别：哦，上一个版本中，索引会定时刷出去，所以还可以理解，现在似乎没有这个必要了， TODO 删掉这个？不用，因为现在他不会在被调用了，仅在flush的时候才会被调用
   *
   * @return how much memory was freed.
   */
  protected long clearFeatureExtractor() {
    return indexFeatureExtractor == null ? 0 : indexFeatureExtractor.clear();
  }

  /**
   * Not that, this method will remove all data and feature. If this method is called, all other
   * methods will be invalid.
   */
  public void closeAndRelease() {
    if (indexFeatureExtractor != null) {
      indexFeatureExtractor.closeAndRelease();
    }
    serializeIndexAndFlush();
  }

  /**
   * 索引将自己的东西刷出去
   */
  protected abstract void serializeIndexAndFlush();

  /**
   * This method serialize information of index and preprocessor into an {@code OutputStream}. It's
   * called when the index file will be close. The information will be back in type of {@code
   * ByteBuffer} when next creation.
   */
  public ByteBuffer serializeFeatureExtractor() throws IOException {
    return indexFeatureExtractor.serializePrevious();
  }

  /**
   * This method is called when a flush task totally finished.
   */
  public void endFlushTask() {
    indexFeatureExtractor.clearProcessedSrcData();
  }

  /**
   * return how much memory is increased for each point processed. It's an amortized estimation,
   * which should consider both {@linkplain IndexFeatureExtractor#getAmortizedSize()} and the
   * <b>index expansion rate</b>.
   */
  public int getAmortizedSize() {
    return indexFeatureExtractor == null ? 0 : indexFeatureExtractor.getAmortizedSize();
  }

  public abstract QueryDataSet query(Map<String, Object> queryProps, IIndexUsable iIndexUsable,
      QueryContext context, IIndexRefinePhaseOptimize refinePhaseOptimizer, boolean alignedByTime)
      throws QueryIndexException;

  /**
   * Initial parameters by query, check if all query conditions and function types are supported
   *
   * @param queryConditions query conditions
   * @throws IllegalIndexParamException when conditions or funcs are not supported
   */
//  public abstract void initQuery(Map<String, Object> queryConditions,
//      List<IndexFuncResult> indexFuncResults) throws UnsupportedIndexFuncException;

  /**
   * query on path with parameters, return the candidate list. return null is regarded as Nothing to
   * be pruned.
   *
   * @return null means nothing to be pruned
   */
//  public abstract List<Identifier> queryByIndex(ByteBuffer indexChunkData)
//      throws IndexManagerException;

  /**
   * IndexPreprocessor has preprocess a new sequence, produce L1, L2 or L3 features as user's
   * configuration. Calculates functions you support and fill into {@code funcResult}.
   *
   * Returns {@code true} if all calculations of this function have been completed. If so, this
   * AggregateResult will not be called next time.
   *
   * @throws UnsupportedOperationException If you meet an unsupported AggregateResult
   */
//  public abstract int postProcessNext(List<IndexFuncResult> indexFuncResults)
//      throws QueryIndexException;

  /**
   * the file is no more needed. Stop ongoing construction and flush operations, clear memory
   * directly and delete the index file.
   */
  public abstract void delete();


  public IndexType getIndexType() {
    return indexType;
  }

  public boolean newTsFileCreated() {
    return false;
  }

  public boolean storageGroupLoaded() {
    return false;
  }

  public boolean newOrderedDataPoint() {
    return false;
  }

  public boolean newOutOfOrderedDataPoint() {
    return false;
  }

  public boolean memDataFlushed() {
    return false;
  }

  public boolean TsFileClosed() {
    return false;
  }

  public boolean mergeFinished() {
    return false;
  }

  @Override
  public String toString() {
    return indexType.toString();
  }

  protected QueryDataSet constructSearchDataset(List<DistSeries> res,
      boolean alignedByTime, int nMaxReturnSeries)
      throws QueryIndexException {
    if (alignedByTime) {
      throw new QueryIndexException("Unsupported alignedByTime result");
    }
    // make result paths and types
    nMaxReturnSeries = Math.min(nMaxReturnSeries, res.size());
    List<PartialPath> paths = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    Map<String, Integer> pathToIndex = new HashMap<>();
    int nMinLength = Integer.MAX_VALUE;
    for (int i = 0; i < nMaxReturnSeries; i++) {
      PartialPath series = res.get(i).partialPath;
      paths.add(series);
      pathToIndex.put(series.getFullPath(), i);
      types.add(tsDataType);
      if (res.get(i).tvList.size() < nMinLength) {
        nMinLength = res.get(i).tvList.size();
      }
    }
    IndexQueryDataSet dataSet = new IndexQueryDataSet(paths, types, pathToIndex);
    if (nMaxReturnSeries == 0) {
      return dataSet;
    }
    for (int row = 0; row < nMinLength; row++) {
      RowRecord record = new RowRecord(row);
      for (int col = 0; col < nMaxReturnSeries; col++) {
        TVList tvList = res.get(col).tvList;
        record.addField(IndexUtils.getValue(tvList, row), tsDataType);
      }
      dataSet.putRecord(record);
    }
    return dataSet;
  }

}