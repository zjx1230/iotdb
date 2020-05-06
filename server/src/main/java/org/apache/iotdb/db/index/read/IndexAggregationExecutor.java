package org.apache.iotdb.db.index.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class IndexAggregationExecutor extends AggregationExecutor {

  private final Map<String, String> props;
  private final IndexType indexType;

  public IndexAggregationExecutor(
      QueryIndexPlan queryIndexPlan) {
    super(queryIndexPlan);
    indexType = queryIndexPlan.getIndexType();
    props = queryIndexPlan.getProps();
  }

  /**
   * Compared to super method, it create IndexAggregateResult instead of Other AggregateResult
   *
   * @param pathIdx the idx of path
   * @param tsDataType path type
   */
  @Override
  protected AggregateResult getAggregateResult(int pathIdx, TSDataType tsDataType) {
    IndexFunc indexFunc = IndexFunc.getIndexFunc(aggregations.get(pathIdx));
    return new IndexAggregateResult(indexFunc, tsDataType, indexType);
  }



}
