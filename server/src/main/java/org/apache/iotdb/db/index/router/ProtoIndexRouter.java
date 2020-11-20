package org.apache.iotdb.db.index.router;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
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
  private Map<String, Set<String>> sgToFullPathMap;
  private Map<String, Set<PartialPath>> sgToWildCardPathMap;
  private MManager mManager;
  private File routerFile;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private boolean unmodifiable;

  ProtoIndexRouter(String routerFileDir) {
    this(false);
    this.routerFile = SystemFileFactory.INSTANCE.getFile(routerFileDir + File.separator + "router");
    mManager = MManager.getInstance();

  }

  private ProtoIndexRouter(boolean unmodifiable) {
    this.unmodifiable = unmodifiable;
    fullPathProcessorMap = new ConcurrentHashMap<>();
    sgToFullPathMap = new ConcurrentHashMap<>();
    sgToWildCardPathMap = new HashMap<>();
    wildCardProcessorMap = new HashMap<>();
  }

  private ProtoIndexRouter() {
    this(true);
  }

  @Override
  public void serializeAndClose() {
    try (OutputStream outputStream = new FileOutputStream(routerFile)) {
      ReadWriteIOUtils.write(fullPathProcessorMap.size(), outputStream);
      for (Entry<String, Pair<Map<IndexType, IndexInfo>, IndexProcessor>> entry : fullPathProcessorMap
          .entrySet()) {
        String indexSeries = entry.getKey();
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> v = entry.getValue();
        ReadWriteIOUtils.write(indexSeries, outputStream);
        ReadWriteIOUtils.write(v.left.size(), outputStream);
        for (Entry<IndexType, IndexInfo> e : v.left.entrySet()) {
          IndexInfo indexInfo = e.getValue();
          indexInfo.serialize(outputStream);
        }
        if (v.right != null) {
          v.right.close();
        }
      }

      ReadWriteIOUtils.write(wildCardProcessorMap.size(), outputStream);
      for (Entry<PartialPath, Pair<Map<IndexType, IndexInfo>, IndexProcessor>> entry : wildCardProcessorMap
          .entrySet()) {
        PartialPath indexSeries = entry.getKey();
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> v = entry.getValue();
        ReadWriteIOUtils.write(indexSeries.getFullPath(), outputStream);
        ReadWriteIOUtils.write(v.left.size(), outputStream);
        for (Entry<IndexType, IndexInfo> e : v.left.entrySet()) {
          IndexInfo indexInfo = e.getValue();
          indexInfo.serialize(outputStream);
        }
        if (v.right != null) {
          v.right.close();
        }
      }
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  @Override
  public void deserializeAndReload(CreateIndexProcessorFunc func) {
    if (!routerFile.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(routerFile)) {
      int fullSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < fullSize; i++) {
        String indexSeries = ReadWriteIOUtils.readString(inputStream);
        int indexTypeSize = ReadWriteIOUtils.readInt(inputStream);
        for (int j = 0; j < indexTypeSize; j++) {
          IndexInfo indexInfo = IndexInfo.deserialize(inputStream);
          addIndexIntoRouter(new PartialPath(indexSeries), indexInfo, func);
        }
      }

      int wildcardSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < wildcardSize; i++) {
        String indexSeries = ReadWriteIOUtils.readString(inputStream);
        int indexTypeSize = ReadWriteIOUtils.readInt(inputStream);
        for (int j = 0; j < indexTypeSize; j++) {
          IndexInfo indexInfo = IndexInfo.deserialize(inputStream);
          addIndexIntoRouter(new PartialPath(indexSeries), indexInfo, func);
        }
      }
    } catch (MetadataException | IOException e) {
      logger.error("Error when deserialize router. Given up.", e);
    }
  }

  @Override
  public IIndexRouter getRouterByStorageGroup(String storageGroupPath) {
    lock.writeLock().lock();
    try {
      ProtoIndexRouter res = new ProtoIndexRouter();
      if (sgToWildCardPathMap.containsKey(storageGroupPath)) {
        for (PartialPath partialPath : sgToWildCardPathMap.get(storageGroupPath)) {
          res.wildCardProcessorMap.put(partialPath, this.wildCardProcessorMap.get(partialPath));
        }
      }
      if (sgToFullPathMap.containsKey(storageGroupPath)) {
        for (String fullPath : sgToFullPathMap.get(storageGroupPath)) {
          res.fullPathProcessorMap.put(fullPath, this.fullPathProcessorMap.get(fullPath));
        }
      }
      return res;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getIndexNum() {
    return fullPathProcessorMap.size() + wildCardProcessorMap.size();
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
    if (unmodifiable) {
      throw new MetadataException("cannot add index to unmodifiable router");
    }
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    IndexType indexType = indexInfo.getIndexType();
    // record the relationship between storage group and the
    StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
    String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();

    // add to pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (!fullPathProcessorMap.containsKey(fullPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          IndexProcessor processor = func.act(partialPath, infoMap);
          fullPathProcessorMap.put(fullPath, new Pair<>(infoMap, processor));
        }
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = fullPathProcessorMap.get(fullPath);
        pair.left.put(indexType, indexInfo);
        // add to sg
        Set<String> indexSeriesSet = new HashSet<>();
        Set<String> preSet = sgToFullPathMap.putIfAbsent(storageGroupPath, indexSeriesSet);
        if (preSet != null) {
          indexSeriesSet = preSet;
        }
        indexSeriesSet.add(fullPath);
      } else {
        if (!wildCardProcessorMap.containsKey(partialPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          IndexProcessor processor = func.act(partialPath, infoMap);
          wildCardProcessorMap.put(partialPath, new Pair<>(infoMap, processor));
        }
        Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = wildCardProcessorMap
            .get(partialPath);
        pair.left.put(indexType, indexInfo);
        // add to sg
        Set<PartialPath> indexSeriesSet = new HashSet<>();
        Set<PartialPath> preSet = sgToWildCardPathMap.putIfAbsent(storageGroupPath, indexSeriesSet);
        if (preSet != null) {
          indexSeriesSet = preSet;
        }
        indexSeriesSet.add(partialPath);
      }
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public boolean removeIndexFromRouter(PartialPath partialPath, IndexType indexType)
      throws MetadataException {
    if (unmodifiable) {
      throw new MetadataException("cannot remove index from unmodifiable router");
    }
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    // record the relationship between storage group and the index processors
    StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
    String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();

    // remove from pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (fullPathProcessorMap.containsKey(fullPath)) {
          Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = fullPathProcessorMap.get(fullPath);
          pair.left.remove(indexType);
          if (pair.left.isEmpty()) {
            sgToFullPathMap.get(storageGroupPath).remove(fullPath);
          }
        }
      } else {
        if (wildCardProcessorMap.containsKey(partialPath)) {
          Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair = wildCardProcessorMap
              .get(partialPath);
          pair.left.remove(indexType);
          if (pair.left.isEmpty()) {
            sgToWildCardPathMap.get(storageGroupPath).remove(partialPath);
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }


  @Override
  public Iterable<Pair<Map<IndexType, IndexInfo>, IndexProcessor>> getAllIndexProcessorsAndInfo() {
    List<Pair<Map<IndexType, IndexInfo>, IndexProcessor>> res = new ArrayList<>(
        wildCardProcessorMap.size() + fullPathProcessorMap.size());
    wildCardProcessorMap.forEach((k, v) -> res.add(v));
    fullPathProcessorMap.forEach((k, v) -> res.add(v));
    return res;
  }

  @Override
  public Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path) {
    List<IndexProcessor> res = new ArrayList<>();
    if (fullPathProcessorMap.containsKey(path.getFullPath())) {
      res.add(fullPathProcessorMap.get(path.getFullPath()).right);
    } else {
      wildCardProcessorMap.forEach((k, v) -> {
        if (k.matchFullPath(path)) {
          res.add(v.right);
        }
      });
    }
    return res;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterable<Pair<Map<IndexType, IndexInfo>, IndexProcessor>> all = getAllIndexProcessorsAndInfo();
    for (Pair<Map<IndexType, IndexInfo>, IndexProcessor> pair : all) {
      sb.append(pair.toString()).append(";");
    }
    return sb.toString();
  }
}
