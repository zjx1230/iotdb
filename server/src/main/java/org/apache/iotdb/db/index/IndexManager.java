/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.pool.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  private IndexBuildTaskPoolManager indexBuildPool = IndexBuildTaskPoolManager.getInstance();
  private MManager mManager;
//  private static Map<IndexType, IIndex> indexMap = new HashMap<>();

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


  private Map<String, IndexProcessor> indexProcessorMap;

  private Thread writeThread;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Runnable forceWriteTask = () -> {
//    while (true) {
//      if (Thread.interrupted()) {
//        logger.info("Interrupted, the index write thread will exit.");
//        return;
//      }
//      if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
//        logger.warn("system mode is read-only, the index write thread will exit.");
//        return;
//      }
//      for (IIndex indexProcessor : nodeMap.values()) {
//        try {
//          indexProcessor.forceSync();
//        } catch (IOException e) {
//          logger.error("Cannot force {}, because ", indexProcessor, e);
//        }
//      }
//      try {
//        Thread.sleep(config.getForceWalPeriodInMs());
//      } catch (InterruptedException e) {
//        logger.info("WAL force thread exits.");
//        Thread.currentThread().interrupt();
//        break;
//      }
//    }
  };


  public IndexProcessor getProcessor(String path) {
    IndexProcessor indexProcessor = indexProcessorMap.get(path);
    if (indexProcessor == null) {
      indexProcessor = initializeIndexProcessor(path);
      IndexProcessor oldProcessor = indexProcessorMap.putIfAbsent(path, indexProcessor);
      if (oldProcessor != null) {
        return oldProcessor;
      }
    }
    return indexProcessor;
  }

  public IndexProcessor initializeIndexProcessor(String path) {
    throw new NotImplementedException();
  }

  public void removeIndexProcessor(String identifier) throws IOException, IndexManagerException {
    IndexProcessor processor = indexProcessorMap.remove(identifier);
    if (processor != null) {
      processor.close();
    }
  }

  public void close() {
    logger.info("IndexManager won't be closed alone, but with StorageEngine and MManger.");
    throw new NotImplementedException("index processor close");
  }

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
    indexProcessorMap = new ConcurrentHashMap<>();
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




  /**
   * you can refer to MManger. We needn't add write lock since it's controlled by MManager.
   */



}
