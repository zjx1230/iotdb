package org.apache.iotdb.db.index.router;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The index involve
 */
public class ProtoIndexRouter implements IIndexRouter {

  private static final Logger logger = LoggerFactory.getLogger(ProtoIndexRouter.class);

  /**
   * index series path -> index processor
   */
  private Map<String, Pair<Map<IndexType, IndexInfo>, IndexProcessor>> fullPathProcessorMap;
  private Map<PartialPath, Pair<Map<IndexType, IndexInfo>, IndexProcessor>> wildCardProcessorMap;
  private Map<String, List<String>> sgToIndexSeriesMap;
  private final MManager mManager;
  private final File routerFile;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  ProtoIndexRouter(String routerFileDir) {
    this.routerFile = SystemFileFactory.INSTANCE.getFile(routerFileDir + File.separator + "router");
    fullPathProcessorMap = new ConcurrentHashMap<>();
    sgToIndexSeriesMap = new ConcurrentHashMap<>();
    mManager = MManager.getInstance();
  }

  @Override
  public void serialize() {
    IndexUtils.breakDown("close所有processor");
    try (ObjectOutputStream routerOutputStream = new ObjectOutputStream(
        new FileOutputStream(routerFile, false))) {
      routerOutputStream.writeObject(this);
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  @Override
  public void deserialize(CreateIndexProcessorFunc func) {
    IndexUtils.breakDown("复原所有processor");
    if (!routerFile.exists()) {
      return;
    }
    try (ObjectInputStream routerInputStream = new ObjectInputStream(
        new FileInputStream(routerFile))) {
      ProtoIndexRouter p = (ProtoIndexRouter) routerInputStream.readObject();
      this.fullPathProcessorMap = p.fullPathProcessorMap;
      this.sgToIndexSeriesMap = p.sgToIndexSeriesMap;
    } catch (IOException | ClassNotFoundException e) {
      logger.error("Error when deserialize router. Given up.", e);
    }
  }

  @Override
  public IIndexRouter getRouterByStorageGroup(String storageGroupPath) {
    return this;
  }

  @Override
  public Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path) {
    List<IndexProcessor> res = new ArrayList<>();
    if (fullPathProcessorMap.containsKey(path.getFullPath())) {
      res.add(fullPathProcessorMap.get(path.getFullPath()).right);
    } else {
      wildCardProcessorMap.forEach((k, v) -> {
        if (k.matchFullPath(path)) {
          res.add(fullPathProcessorMap.get(path.getFullPath()).right);
        }
      });
    }
    return res;
  }

  @Override
  public boolean hasIndexProcessor(PartialPath indexSeriesPath) {
    if (fullPathProcessorMap.containsKey(indexSeriesPath.getFullPath())) {
      return true;
    }
    for (Entry<PartialPath, Pair<Map<IndexType, IndexInfo>, IndexProcessor>> entry : wildCardProcessorMap
        .entrySet()) {
      PartialPath k = entry.getKey();
      if (k.matchFullPath(indexSeriesPath)) {
        return true;
      }
    }
    return false;
  }


  @Override
  public boolean addIndexIntoRouter(PartialPath partialPath, IndexInfo indexInfo,
      CreateIndexProcessorFunc func) throws MetadataException {
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    IndexType indexType = indexInfo.getIndexType();
    // add to pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (!fullPathProcessorMap.containsKey(fullPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          IndexProcessor processor = func.act(partialPath);
          fullPathProcessorMap.put(fullPath, new Pair<>(infoMap, processor);
        }
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = fullPathProcessorMap.get(fullPath);
        pair.left.put(indexType, indexInfo);
      } else {
        if (!wildCardProcessorMap.containsKey(partialPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          IndexProcessor processor = func.act(partialPath);
          wildCardProcessorMap.put(partialPath, new Pair<>(infoMap, processor);
        }
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = wildCardProcessorMap
            .get(partialPath);
        pair.left.put(indexType, indexInfo);
      }

      // record the relationship between storage group and the
      StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
      String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();
      List<String> list = new ArrayList<>();
      List<String> preList = sgToIndexSeriesMap.putIfAbsent(storageGroupPath, list);
      if (preList != null) {
        list = preList;
      }
      list.add(partialPath.getFullPath());

    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public boolean removeIndexFromRouter(PartialPath partialPath, IndexType indexType)
      throws MetadataException {
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    // remove from pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (fullPathProcessorMap.containsKey(fullPath)) {
          Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = fullPathProcessorMap.get(fullPath);
          pair.left.remove(indexType);
        }
      } else {
        if (wildCardProcessorMap.containsKey(partialPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = wildCardProcessorMap
              .get(partialPath);
          pair.left.remove(indexType);
        }
      }

      // record the relationship between storage group and the
      StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
      String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();
      List<String> list = new ArrayList<>();
      List<String> preList = sgToIndexSeriesMap.putIfAbsent(storageGroupPath, list);
      if (preList != null) {
        list = preList;
      }
      list.add(partialPath.getFullPath());

    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }


  @Override
  public Iterable<IndexProcessor> getAllIndexProcessors() {
    throw new UnsupportedOperationException();
  }

}
