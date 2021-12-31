package org.apache.iotdb.tsfile.read.filter.codegen;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public abstract class DynamicFilter implements Filter {

  protected final Filter delegate;

  public DynamicFilter(Filter delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return this.delegate.satisfy(statistics);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return this.delegate.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return this.delegate.containStartEndTime(startTime, endTime);
  }

  @Override
  public Filter copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FilterSerializeId getSerializeId() {
    throw new UnsupportedOperationException();
  }
}
