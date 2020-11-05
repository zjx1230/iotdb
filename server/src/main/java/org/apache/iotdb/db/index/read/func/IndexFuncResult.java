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
package org.apache.iotdb.db.index.read.func;

import static org.apache.iotdb.db.query.aggregation.AggregationType.INDEX;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * For scalar-type result, each column has several results, and each result is a scalar.
 *
 * For tensor-type result, each column has several results, and each result is a
 * <tt>multi-dimensional</tt> tensor.
 *
 * For example, the timestamps and points of all sequences with distance less than 10 to query.
 *
 * The result volume may be very large, so it is necessary to consider many issues such as
 * batch-fetch, whether can we reuse existing operations such as GROUP BY and LIMIT, so it is at the
 * discussion stage up to now.
 */
public class IndexFuncResult extends AggregateResult {

  private boolean isTensor;
  private final IndexFunc indexFunc;
  private TSDataType indexFuncDataType;
  private final List<List<Object>> tensorList;
  private final List<Object> scalarList;

  private int firstIdx = 0;
  private int secondIdx = 0;

  public IndexFuncResult(IndexFunc indexFunc, TSDataType tsDataType) {
    super(tsDataType, INDEX);
    this.indexFunc = indexFunc;
    this.tensorList = new ArrayList<>();
    this.scalarList = new ArrayList<>();

  }

  public void addToNewestTensor(Object subTensor) {
    if (!tensorList.isEmpty()) {
      tensorList.get(tensorList.size() - 1).add(subTensor);
    }
  }

  public void addTensor(List<Object> tensor) {
    tensorList.add(tensor);

  }

  public void addScalar(Object scalar) {
    scalarList.add(scalar);
  }

  public TSDataType getIndexFuncDataType() {
    return indexFuncDataType;
  }

  public void setIndexFuncDataType(
      TSDataType indexFuncDataType) {
    this.indexFuncDataType = indexFuncDataType;
  }

  public boolean isTensor() {
    return isTensor;
  }

  public void setIsTensor(boolean tensor) {
    isTensor = tensor;
  }

  public IndexFunc getIndexFunc() {
    return indexFunc;
  }

  public void prepareToOut() {
    firstIdx = 0;
    secondIdx = 0;
  }

  private boolean findNextAvailable() {
    while (firstIdx < tensorList.size()) {
      List<Object> first = tensorList.get(firstIdx);
      if (secondIdx < first.size()) {
        return true;
      } else {
        secondIdx = 0;
        firstIdx++;
      }
    }
    return false;
  }

  /**
   * [ID, value]. If no next, return [-1, null]
   */
  public Pair<Integer, Object> getNextPair() {
    Pair<Integer, Object> res = new Pair<>(-1, null);
    if (isTensor && findNextAvailable()) {
      res.left = firstIdx;
      res.right = tensorList.get(firstIdx).get(secondIdx);
      secondIdx++;
    } else {
      if (firstIdx < scalarList.size()) {
        res.left = firstIdx;
        res.right = scalarList.get(firstIdx);
      }
      firstIdx++;
    }
    return res;
  }


  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public String toString() {
    return isTensor ? tensorList.toString() : scalarList.toString();
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) throws QueryProcessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFinalResult() {
    // TODO throw new UnsupportedOperationException();
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }
}
