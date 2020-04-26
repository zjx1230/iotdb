package org.apache.iotdb.db.index.algorithm;

import static org.apache.iotdb.db.index.common.IndexConstant.DEFAULT_PROP_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_RANGE_STRATEGY;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.DataFileInfo;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.indexrange.IndexRangeStrategy;
import org.apache.iotdb.db.index.indexrange.IndexRangeStrategyType;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Each StorageGroupProcessor contains a IndexProcessor, and each IndexProcessor can contain more
 * than one IIndex. Each type of Index corresponds one IIndex.
 */
public abstract class IoTDBIndex {

  protected final IndexType indexType;
  protected final long startTime;
  protected final Map<String, String> props;
  private IndexRangeStrategy strategy;

  public IoTDBIndex(IndexInfo indexInfo) {
    this.indexType = indexInfo.getIndexType();
    this.startTime = indexInfo.getTime();
    this.props = indexInfo.getProps();
    parsePropsAndInit(this.props);
  }

  private void parsePropsAndInit(Map<String, String> props) {
    // Strategy
    this.strategy = IndexRangeStrategyType
        .getIndexStrategy(props.getOrDefault(INDEX_RANGE_STRATEGY, DEFAULT_PROP_NAME));
  }

  public boolean checkNeedIndex(TVList sortedTVList, long configStartTime) {
    return strategy.needBuildIndex(sortedTVList, configStartTime);
  }


  /**
   * Given one new file contain path, create the index file Call this method when the close
   * operation has completed.
   *
   * @param path the time series to be indexed
   * @param newFile the new file contain path
   * @param parameters other parameters
   */
  public abstract boolean build(Path path, DataFileInfo newFile, Map<String, Object> parameters)
      throws IndexManagerException;

  public abstract boolean flush(Path path, DataFileInfo newFile, Map<String, Object> parameters)
      throws IndexManagerException;


  /**
   * query on path with parameters, return result by limitSize
   *
   * @param path the path to be queried
   * @param parameters the query request with all parameters
   * @param nonUpdateIntervals the query request with all parameters
   * @param limitSize the limitation of number of answers
   * @return the query response
   */
  public abstract Object queryByIndex(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals,
      int limitSize)
      throws IndexManagerException;

  /**
   * query on path with parameters, return result by limitSize
   *
   * @param path the path to be queried
   * @param parameters the query request with all parameters
   * @param nonUpdateIntervals the query request with all parameters
   * @param limitSize the limitation of number of answers
   * @return the query response
   */
  public abstract Object queryByScan(Path path, List<Object> parameters,
      List<Pair<Long, Long>> nonUpdateIntervals,
      int limitSize)
      throws IndexManagerException;

  /**
   * the file is no more needed. Stop ongoing construction and flush operations, clear memory
   * directly and delete the index file.
   */
  public abstract void delete();
}