package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

import java.util.List;

public interface ISeriesAggregationExecutor {

  public List<AggregateResult> getAggregateResult(QueryContext context, TimeFilter timeFilter);

  public List<Integer> getSeriesIndex();
}
