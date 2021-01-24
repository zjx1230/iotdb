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

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_DATA_DIR_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.META_DIR_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.ROUTER_DIR;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexMessageType;
import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 目前每个索引都可以与一条序列名对应。全序列索引包含通配符，而子序列索引仅支持一条序列名创建。
 */
public class IndexManager implements IndexManagerMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  /**
   * 索引根目录。所有索引元数据文件、索引实例的结构和数据文件均放置在该目录下。在构造函数中指定。
   *
   * 遵循WAL，调用 {@link DirectoryManager#getIndexRootFolder} 方法
   */
  private final String indexRootDirPath;
  /**
   * 索引元数据文件目录，在构造函数中指定。
   */
  private final String indexMetaDirPath;
  /**
   * 路由器文件目录，在构造函数中指定。
   */
  private final String indexRouterDir;
  /**
   * 索引实例文件的目录。在构造函数中指定。
   */
  private final String indexDataDirPath;
  /**
   * 索引实例管理器的构造方法接口（function Interface），在构造函数中指定。在创建索引和系统启动时传入。
   */
  private CreateIndexProcessorFunc createIndexProcessorFunc;
  /**
   * 索引路由器。在构造函数中指定。
   */
  private final IIndexRouter router;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * 给定创建索引的序列名，返回该索引的特征文件路径（目前未使用）。
   *
   * @param path the path on which the index is created, e.g. Root.ery.*.Glu or Root.Wind.d1.Speed.
   * @param indexType the type of index
   * @return the feature directory path for this index.
   */
  private String getFeatureFileDirectory(PartialPath path, IndexType indexType) {
    return indexDataDirPath + File.separator + path.getFullPath() + File.separator + indexType;
  }

  /**
   * 给定创建索引的序列名，返回该索引的数据文件路径（目前未使用）。数据文件指的是索引内存结构序列化到磁盘后产生的文件。目前未被使用
   *
   * @param path the path on which the index is created, e.g. Root.ery.*.Glu or Root.Wind.d1.Speed.
   * @param indexType the type of index
   * @return the feature directory path for this index.
   */
  private String getIndexDataDirectory(PartialPath path, IndexType indexType) {
    return getFeatureFileDirectory(path, indexType);
  }

  /**
   * 执行创建索引命令。由于IoTDB的序列与索引之间存在多对多的复杂映射关系，我们将索引元数据信息交给路由器 {@link IIndexRouter}
   * 来创建和维护。这样IndexManager可以保证代码的稳定。
   *
   * @param indexSeriesList 也许未来会支持在多路径上创建索引，但目前我们仅支持单序列创建。对于全序列索引，是一条包含通配符的路径，对于子序列索引，是一条无通配符的全路径（fullPath）。
   * @param indexInfo 索引信息
   */
  public void createIndex(List<PartialPath> indexSeriesList, IndexInfo indexInfo)
      throws MetadataException {
    if (!indexSeriesList.isEmpty()) {
      router.addIndexIntoRouter(indexSeriesList.get(0), indexInfo, createIndexProcessorFunc);
    }
  }

  /**
   * 执行删除索引命令。参见{@linkplain IndexManager#createIndex}
   *
   * @param indexSeriesList 指定删除索引的序列
   * @param indexType 索引类别
   */
  public void dropIndex(List<PartialPath> indexSeriesList, IndexType indexType)
      throws MetadataException {
    if (!indexSeriesList.isEmpty()) {
      router.removeIndexFromRouter(indexSeriesList.get(0), indexType);
    }
  }

  /**
   * For index techniques which are deeply optimized for IoTDB, the index framework provides a
   * finer-grained interface to inform the index when some IoTDB events occur. Other modules can
   * call this function to pass the specified index message type and other parameters. Indexes can
   * selectively implement these interfaces and register with the index manager for listening. When
   * these events occur, the index manager passes the messages to corresponding indexes.
   *
   * Note that, this function will hack into other modules.
   *
   * @param path where events occur
   * @param indexMsgType the type of index message
   * @param params variable length of parameters
   */
  public void tellIndexMessage(PartialPath path, IndexMessageType indexMsgType, Object... params) {
    throw new UnsupportedOperationException("Not support the advanced Index Messages");
  }

  /**
   * 从代码设计原则上来说，路由器仅负责"寻找" {@link IndexProcessor} 和管理索引信息， 我们应该使用类似 {@code getAllProcessors}
   * 的方法获取所有IndexProcessors并关闭。 然而目前IndexManager并没有任何其他操作，因此我们将 {@link IndexProcessor}
   * 的关闭操作也放在router中。
   */
  private synchronized void close() {
    router.serializeAndClose(true);
  }

  /**
   * 系统启动时，IndexManager检查目录是否创建完整，然后将router反序列化到内存。
   */
  @Override
  public void start() throws StartupException {
    if (!config.isEnableIndex()) {
      return;
    }
    IndexBuildTaskPoolManager.getInstance().start();
    try {
      JMXService.registerMBean(this, ServiceType.INDEX_SERVICE.getJmxName());
      prepareDirectory();
      router.deserializeAndReload(createIndexProcessorFunc);
      deleteDroppedIndexData();
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  /**
   * 由于IoTDB没有正常退出机制，因此这一函数并不会被调用。为了保证不丢失信息，Router需要在每次 createIndex 或 dropIndex 被调用时都将索引元数据序列化到磁盘。
   */
  @Override
  public void stop() {
    if (!config.isEnableIndex()) {
      return;
    }
    close();

    IndexBuildTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.INDEX_SERVICE.getJmxName());
  }


  private IndexManager() {
    indexRootDirPath = DirectoryManager.getInstance().getIndexRootFolder();
    indexMetaDirPath = indexRootDirPath + File.separator + META_DIR_NAME;
    indexRouterDir = indexMetaDirPath + File.separator + ROUTER_DIR;
    indexDataDirPath = indexRootDirPath + File.separator + INDEX_DATA_DIR_NAME;
    createIndexProcessorFunc = (indexSeries, indexInfoMap) -> new IndexProcessor(
        indexSeries, indexDataDirPath + File.separator + indexSeries);
    router = IIndexRouter.Factory.getIndexRouter(indexRouterDir);
  }

  public static IndexManager getInstance() {
    return InstanceHolder.instance;
  }


  /**
   * 当存储组刷新时，构造 {@link IndexMemTableFlushTask} 用于写入。
   *
   * 目前仅当IoTDB执行flush操作时才触发"索引写入"。一个存储组中有多条序列，每条序列上可能创建了子序列索引或全序列索引。 因此，每个存储组可能对应多个{@link
   * IndexProcessor}。存储组中的序列需要寻找它所属的IndexProcessor。这一过程由一个路由器（{@code sgRouter}）完成。
   *
   * 本方法将sgRouter和其他信息构成 {@link IndexMemTableFlushTask}，返回给{@link IndexMemTableFlushTask }使用。
   *
   * @param storageGroupPath 存储组路径。
   * @param sequence 是否是顺序数据。当前实现中，顺序数据则更新索引，乱序数据则仅更新索引对应的可用区间管理器。
   * @see IndexMemTableFlushTask
   */
  public IndexMemTableFlushTask getIndexMemFlushTask(String storageGroupPath, boolean sequence) {
    IIndexRouter sgRouter = router.getRouterByStorageGroup(storageGroupPath);
    return new IndexMemTableFlushTask(sgRouter, sequence);
  }

  /**
   * 索引查询。指定查询序列、索引类型和其他参数，首先对该查询用到的存储组加mergeLock，然后交给对应的IndexProcessor执行查询并返回。
   *
   * 本方法将整个查询过程全部交给索引实例完成，原始数据的获取也由索引在函数内部操作。索引框架不对查询过程进行干预。
   *
   * 所谓最初设想的"索引框架的干预"，指的是"剪枝阶段"由索引实例得到候选集，而"后处理阶段"完全由索引框架完成（即对候选集所对应的原始数据进行遍历）。
   * 这样的方案是简单的。
   *
   * 然而，索引技术有各种优化，强制执行上述策略会影响索引的集成。例如，"剪枝阶段"也可能要访问原始数据（DS-Tree）。允许索引技术访问原始数据的话，那么干脆就不要这些限制。
   *
   * 当我们集成了足够多的索引技术后，可以考虑发现问题，提取共性，改进我们的查询逻辑。
   *
   * @param paths the series to be queried.
   * @param indexType the index type to be queried.
   * @param queryProps the properties of this query
   * @param context the query context
   * @param alignedByTime whether aligned index result by timestamps.
   * @return the index query result
   * @throws QueryIndexException 对于不存在的索引进行查询时抛出异常。TODO 如果允许"无索引剪枝的查询"（NoIndex），则不再为此抛出异常
   * @throws StorageEngineException 对查询所需的StorageGroup加mergeLock时遇到异常则抛出
   */
  public QueryDataSet queryIndex(List<PartialPath> paths, IndexType indexType,
      Map<String, Object> queryProps, QueryContext context, boolean alignedByTime)
      throws QueryIndexException, StorageEngineException {
    if (paths.size() != 1) {
      throw new QueryIndexException("Index allows to query only one path");
    }
    PartialPath queryIndexSeries = paths.get(0);
    IndexProcessorStruct indexProcessorStruct = router
        .startQueryAndCheck(queryIndexSeries, indexType, context);
    List<StorageGroupProcessor> list = StorageEngine.getInstance()
        .mergeLock(indexProcessorStruct.storageGroups);
    try {
      return indexProcessorStruct.processor.query(indexType, queryProps, context, alignedByTime);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
      router.endQuery(indexProcessorStruct.processor.getIndexSeries(), indexType, context);
    }
  }

  /**
   * 如果索引的各种目录不存在，则创建。
   */
  private void prepareDirectory() {
    File rootDir = IndexUtils.getIndexFile(indexRootDirPath);
    if (!rootDir.exists()) {
      rootDir.mkdirs();
    }
    File routerDir = IndexUtils.getIndexFile(indexRouterDir);
    if (!routerDir.exists()) {
      routerDir.mkdirs();
    }
    File metaDir = IndexUtils.getIndexFile(indexMetaDirPath);
    if (!metaDir.exists()) {
      metaDir.mkdirs();
    }
    File dataDir = IndexUtils.getIndexFile(indexDataDirPath);
    if (!dataDir.exists()) {
      dataDir.mkdirs();
    }
  }

  /**
   * 清理掉已被删除的索引。遍历索引数据目录下每个文件夹（文件夹名对应indexSeries）：
   *
   * 如果indexSeries不存在于router，则删除整个IndexProcessor文件夹；
   *
   * 否则获取该Processor创建的所有索引信息（{@code Map<IndexType,IndexInfo>}），然后删除IndexProcessor文件夹下对应的IndexType文件夹。
   */
  private void deleteDroppedIndexData() throws IOException, IllegalPathException {
    for (File processorDataDir : Objects
        .requireNonNull(IndexUtils.getIndexFile(indexDataDirPath).listFiles())) {
      String processorName = processorDataDir.getName();
      Map<IndexType, IndexInfo> infos = router
          .getIndexInfosByIndexSeries(new PartialPath(processorName));
      if (infos.isEmpty()) {
        FileUtils.deleteDirectory(processorDataDir);
      } else {
        for (File indexDataDir : Objects
            .requireNonNull(processorDataDir.listFiles())) {
          if (indexDataDir.isDirectory() && !infos.containsKey(IndexType.valueOf(indexDataDir.getName()))) {
            FileUtils.deleteDirectory(indexDataDir);
          }
        }
      }
    }
  }

  /**
   * 由 {@link IndexManagerMBean} 定义并调用。
   *
   * @return 创建过的IndexProcessor数量。
   */
  @Override
  public int getIndexNum() {
    return router.getIndexNum();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INDEX_SERVICE;
  }


  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static IndexManager instance = new IndexManager();
  }

  /**
   * 以下函数仅在测试时使用。
   *
   * @throws IOException 删除文件异常是抛出
   */
  @TestOnly
  public synchronized void deleteAll() throws IOException {
    logger.info("Start deleting all storage groups' timeseries");
    close();

    File indexMetaDir = IndexUtils.getIndexFile(this.indexMetaDirPath);
    if (indexMetaDir.exists()) {
      FileUtils.deleteDirectory(indexMetaDir);
    }

    File indexDataDir = IndexUtils.getIndexFile(this.indexDataDirPath);
    if (indexDataDir.exists()) {
      FileUtils.deleteDirectory(indexDataDir);
    }
    File indexRootDir = IndexUtils
        .getIndexFile(DirectoryManager.getInstance().getIndexRootFolder());
    if (indexRootDir.exists()) {
      FileUtils.deleteDirectory(indexRootDir);
    }
  }

  @TestOnly
  public IIndexRouter getRouter() {
    return router;
  }
}
