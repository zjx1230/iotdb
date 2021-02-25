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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Each StorageGroupProcessor contains a IndexProcessor, and each IndexProcessor can contain more
 * than one IIndex. Each type of Index corresponds one IIndex.
 */
public abstract class IoTDBIndex {

  protected final PartialPath indexSeries;
  protected final IndexType indexType;
  protected final TSDataType tsDataType;

  protected final Map<String, String> props;
  protected IndexFeatureExtractor indexFeatureExtractor;

  /** Only build index for data later than this timestamp. Not used yet. */
  protected final long confIndexStartTime;

  public IoTDBIndex(PartialPath indexSeries, TSDataType tsDataType, IndexInfo indexInfo) {
    this.indexSeries = indexSeries;
    this.indexType = indexInfo.getIndexType();
    this.confIndexStartTime = indexInfo.getTime();
    this.props = indexInfo.getProps();
    this.tsDataType = tsDataType;
  }

  /**
   * An index should determine which preprocessor it uses and hook it to {@linkplain
   * IoTDBIndex}.indexProcessor. An index should determine which preprocessor it uses and connect to
   * the IndexProcessor.
   *
   * <p>This method is called when IoTDBIndex is created and accept the previous overlapped data in
   * type of {@code ByteBuffer}.
   *
   * <p>Note that, the implementation should call {@code deserializePrevious(ByteBuffer byteBuffer)}
   * after initialize the preprocessor.
   */
  public abstract void initPreprocessor(ByteBuffer previous, boolean inQueryMode);

  /** When this function is called, it means that a new point has been pre-processed. */
  public abstract boolean buildNext() throws IndexManagerException;

  /** 索引将自己的东西刷出去 */
  protected abstract void flushIndex();

  public abstract QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable iIndexUsable,
      QueryContext context,
      IIndexRefinePhaseOptimize refinePhaseOptimizer,
      boolean alignedByTime)
      throws QueryIndexException;

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

  /** This method is called when a flush task totally finished. */
  public void endFlushTask() {
    indexFeatureExtractor.clearProcessedSrcData();
  }

  /**
   * Not that, this method will remove all data and feature. If this method is called, all other
   * methods will be invalid.
   * @return
   */
  public ByteBuffer closeAndRelease() throws IOException {
    flushIndex();
    if (indexFeatureExtractor != null) {
      return indexFeatureExtractor.closeAndRelease();
    }else{
      return ByteBuffer.allocate(0);
    }
  }

  /**
   * return how much memory is increased for each point processed. It's an amortized estimation,
   * which should consider both {@linkplain IndexFeatureExtractor#getAmortizedSize()} and the
   * <b>index expansion rate</b>.
   */
  public int getAmortizedSize() {
    return indexFeatureExtractor == null ? 0 : indexFeatureExtractor.getAmortizedSize();
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public String toString() {
    return indexType.toString();
  }

  protected QueryDataSet constructSearchDataset(
      List<DistSeries> res, boolean alignedByTime, int nMaxReturnSeries)
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
