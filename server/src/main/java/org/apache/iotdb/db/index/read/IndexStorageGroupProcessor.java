package org.apache.iotdb.db.index.read;

import static org.apache.iotdb.db.conf.IoTDBConstant.SEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.UNSEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXING_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexStorageProcessor manages the index of all time series belonging to a StorageGroup. It can
 * joint two adjacent index files between their boundaries.
 */
public class IndexStorageGroupProcessor {

  private static final Logger logger = LoggerFactory.getLogger(IndexStorageGroupProcessor.class);
  private final String seqDir;
  private final String unseqDir;
  private final String storageGroupName;

  /**
   * IndexFileName -> IndexFileProcessor. Each IndexFileProcessor corresponds to an open
   * TSFileProcessor, and different processors belong to different partition ranges.
   */
  private final Map<String, IndexFileProcessor> seqIndexProcessorMap = new ConcurrentHashMap<>();
  private final Map<String, IndexFileProcessor> unseqIndexProcessorMap = new ConcurrentHashMap<>();

  /**
   * After IndexFileProcessor is closed, it will generate a {@linkplain IndexFileResource} and add
   * the resource to corresponding resource list for reading.
   *
   * When the system restart and a IndexStorageGroupProcessor is created, all its resources will be
   * reloaded. Only the first-layer metadata will be loaded, which is slight enough. Only when a
   * query on a time series arrives, its chunk metadata will be read in and put into the chunk
   * metadata cache for efficiency.
   */
  private List<IndexFileResource> unseqResourceList = new ArrayList<>();
  private List<IndexFileResource> seqResourceList = new ArrayList<>();

  private Lock lock;
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public IndexStorageGroupProcessor(String storageGroupName) {
    this.storageGroupName = storageGroupName;

    this.seqDir =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + SEQUENCE_FLODER_NAME
            + File.separator + storageGroupName;
    this.unseqDir = DirectoryManager.getInstance().getIndexRootFolder() + File.separator
        + UNSEQUENCE_FLODER_NAME + File.separator + storageGroupName;
    lock = new ReentrantLock();
    try {
      recover();
    } catch (IOException e) {
      logger.error("recover meet error", e);
    }
  }

  private void recover() throws IOException {
    logger.info("start to recover Index Storage Group  {}", storageGroupName);
    List<IndexFileResource> tmpSeqIndexResources = getAllFiles(seqDir);
    List<IndexFileResource> tmpUnseqIndexResources = getAllFiles(unseqDir);
    seqResourceList.addAll(tmpSeqIndexResources);
    unseqResourceList.addAll(tmpUnseqIndexResources);
    logger.info("finish recovering Index Storage Group {}", storageGroupName);
  }

  private List<IndexFileResource> getAllFiles(String indexDir) throws IOException {
    List<IndexFileResource> res = new ArrayList<>();
    List<File> indexFiles = new ArrayList<>();
    File fileFolder = fsFactory.getFile(indexDir);
    if (!fileFolder.exists()) {
      return res;
    }
    File[] subFiles = fileFolder.listFiles();
    if (subFiles != null) {
      for (File partitionFolder : subFiles) {
        continueFailedRenames(partitionFolder);
        if (!partitionFolder.isDirectory()) {
          logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          continue;
        }
        Collections.addAll(indexFiles,
            fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), INDEXED_SUFFIX));
      }
    }

    indexFiles.sort(this::compareFileName);
    for (File f : indexFiles) {
      res.add(new IndexFileResource(f.getAbsolutePath()));
    }
    return res;
  }

  /**
   * <p>Refer to {@code StorageGroupProcessor#compareFileName(java.io.File, java.io.File)}</p>
   *
   * <p>Name format: {systemTime}-{versionNum}-{mergeNum}.index</p>
   */
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(INDEXED_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(INDEXED_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }


  /**
   * Up to now, we adopt a naive recovery strategy. If the index file has been generated completely,
   * rename it. Otherwise, delete the index file directly. This will not affect the correctness of
   * the index's query results
   */
  private void continueFailedRenames(File fileFolder) throws IOException {
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), INDEXING_SUFFIX);
    if (files != null) {
      for (File indexFile : files) {
        // check fails
        TsFileInput indexInput = FSFactoryProducer
            .getFileInputFactory().getTsFileInput(indexFile.getPath());
        if (IndexIOReader.checkTailComplete(indexInput)) {
          File normalResource = fsFactory.getFile(indexFile.getPath().replace(
              INDEXING_SUFFIX, INDEXED_SUFFIX));
          indexFile.renameTo(normalResource);
        } else {
          indexFile.delete();
        }
        indexInput.close();
      }
    }
  }

  /**
   * In current version, the index file has the same path format as the corresponding tsfile except
   * that tsfile_base_dir and the ".tsfile" suffix are replaced by index_base_dir and ".index".
   * <br>
   * i.e., index_base_dir / [un]sequence / storage_group_name / partition_id / tsfile_name.index
   * <br>
   * e.g.<br>
   *
   * tsfile: data/sequence/root.idx1/0/1587719150666-1-0.tsfile index file:
   * index/sequence/root.idx1/0/1587719150666-1-0.index
   *
   * Since the total size of index files may be large, the base_dir may be selected from a list of
   * param, just like {@linkplain DirectoryManager#getNextFolderForSequenceFile()
   * getNextFolderForSequenceFile} in future.
   */
  private static String getIndexFileName(String tsfileNameWithSuffix) {

    int tsfileLen = tsfileNameWithSuffix.length() - TsFileConstant.TSFILE_SUFFIX.length();
    return tsfileNameWithSuffix.substring(0, tsfileLen);
  }

  public IndexFileProcessor createIndexFileProcessor(boolean sequence, long partitionId,
      String tsFileName) {

    Map<String, IndexFileProcessor> processorMap = getSeqOrUnseqProcessorMap(sequence);
    String indexParentDir = sequence ? seqDir : unseqDir;
    indexParentDir += File.separator + partitionId;

    if (SystemFileFactory.INSTANCE.getFile(indexParentDir).mkdirs()) {
      logger.info("create the index folder {}", indexParentDir);
    }
    String fullFilePath = indexParentDir + File.separator + getIndexFileName(tsFileName);

    IndexFileProcessor fileProcessor = processorMap.get(fullFilePath);
    if (fileProcessor == null) {
      fileProcessor = new IndexFileProcessor(storageGroupName, indexParentDir, fullFilePath,
          sequence, this::addIndexFileResources);
      IndexFileProcessor oldProcessor = processorMap.putIfAbsent(fullFilePath, fileProcessor);
      if (oldProcessor != null) {
        return oldProcessor;
      }
    }
    return fileProcessor;
  }


  private void addIndexFileResources(boolean sequence, IndexFileResource resources) {
    if (sequence) {
      seqResourceList.add(resources);
    } else {
      unseqResourceList.add(resources);
    }
  }


  private Map<String, IndexFileProcessor> getSeqOrUnseqProcessorMap(boolean sequence) {
    return sequence ? seqIndexProcessorMap : unseqIndexProcessorMap;
  }

  private List<IndexFileResource> getSeqOrUnseqResources(boolean sequence) {
    return sequence ? seqResourceList : unseqResourceList;
  }

  public void removeIndexProcessor(String identifier, boolean sequence) throws IOException {
    Map<String, IndexFileProcessor> processorMap = getSeqOrUnseqProcessorMap(sequence);
    IndexFileProcessor processor = processorMap.remove(identifier);
    if (processor != null) {
      processor.close();
    }
  }

  public synchronized void close() throws IOException {
    for (Entry<String, IndexFileProcessor> entry : seqIndexProcessorMap.entrySet()) {
      IndexFileProcessor processor = entry.getValue();
      processor.close();
    }

    for (Entry<String, IndexFileProcessor> entry : unseqIndexProcessorMap.entrySet()) {
      IndexFileProcessor processor = entry.getValue();
      processor.close();
    }
  }

  public synchronized void deleteAll() throws IOException {
    logger.info("Start deleting all storage groups' timeseries");
    close();
    // delete all index files
    for (Entry<String, IndexFileProcessor> entry : seqIndexProcessorMap.entrySet()) {
      IndexFileProcessor processor = entry.getValue();
      File file = new File(processor.getIndexFilePath());
      if (file.exists()) {
        file.delete();
      }
    }
    for (Entry<String, IndexFileProcessor> entry : unseqIndexProcessorMap.entrySet()) {
      IndexFileProcessor processor = entry.getValue();
      File file = new File(processor.getIndexFilePath());
      if (file.exists()) {
        file.delete();
      }
    }
    File seqFile = new File(seqDir);
    if (seqFile.exists()) {
      FileUtils.deleteDirectory(seqFile);
    }
    File unseqFile = new File(unseqDir);
    if (unseqFile.exists()) {
      FileUtils.deleteDirectory(unseqFile);
    }
    clear();
  }

  private synchronized void clear() {
    seqIndexProcessorMap.clear();
    unseqIndexProcessorMap.clear();
    seqResourceList.clear();
    unseqResourceList.clear();
  }

  public List<IndexChunkMeta> getIndexMetadata(boolean sequence, String seriesPath,
      IndexType indexType) throws IOException {
    List<IndexChunkMeta> res = new ArrayList<>();
    List<IndexFileResource> resourceList = getSeqOrUnseqResources(sequence);
    for (IndexFileResource indexFileResource : resourceList) {
      res.addAll(indexFileResource.getChunkMetas(seriesPath, indexType));
    }
    return res;
  }

  @FunctionalInterface
  public interface AddIndexFileResourcesCallBack {

    void call(boolean sequence, IndexFileResource caller);
  }
}
