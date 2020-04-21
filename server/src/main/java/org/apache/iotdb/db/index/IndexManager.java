package org.apache.iotdb.db.index;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.pool.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  private IndexBuildTaskPoolManager indexBuildPool = IndexBuildTaskPoolManager.getInstance();
  private static Map<IndexType, IIndex> indexMap = new HashMap<>();

  @Override
  public void start() throws StartupException {
    IndexBuildTaskPoolManager.getInstance().start();
    try {
      JMXService.registerMBean(this, ServiceType.INDEX_SERVICE.getJmxName());
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    IndexBuildTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.INDEX_SERVICE.getJmxName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INDEX_SERVICE;
  }


  private IndexManager() {
  }

  public static IndexManager getInstance() {
    return IndexManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static IndexManager instance = new IndexManager();
  }

  public String toString() {
    return String
        .format("the size of Index Build Task Pool: %d", indexBuildPool.getWorkingTasksNumber());
  }

  static {
//    IndexBuildTaskPoolManager.getInstance().start();
//    try {
//      JMXService.registerMBean(this, ServiceType.FLUSH_SERVICE.getJmxName());
//    } catch (Exception e) {
//      throw new StartupException(this.getID().getName(), e.getMessage());
//    }
//    indexMap.put(IndexType.ELB, KvMatchIndex.getInstance());
  }

  public static IIndex getIndexInstance(IndexType indexType) {
    return indexMap.get(indexType);
  }


}
