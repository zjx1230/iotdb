package org.apache.iotdb.db.index.read;

import static org.apache.iotdb.db.query.aggregation.AggregationType.INDEX;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class IndexFuncResult extends AggregateResult {

  private final IndexFunc indexFunc;
  private final IndexType indexType;

  public IndexFuncResult(IndexFunc indexFunc, TSDataType tsDataType, IndexType indexType) {
    super(tsDataType, INDEX);
    this.indexFunc = indexFunc;
    this.indexType = indexType;
  }

  @Override
  public Object getResult() {
    return null;
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
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    throw new UnsupportedOperationException();
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
