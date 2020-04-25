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

import static org.apache.iotdb.db.index.IndexManager.INDEXING_SUFFIX;
import static org.apache.iotdb.db.index.IndexManager.INDEXED_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask.StartFlushGroupIOTask;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class IndexFileProcessor implements Comparable<IndexFileProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexFileProcessor.class);
  private String indexFullPath;
  private final String storageGroupName;

  //  private String path;
  private String indexParentDir;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private static long firstLayerBufferSize = IoTDBDescriptor.getInstance().getConfig()
      .getIndexBufferSize();
  private Map<IndexType, IIndex> indexMap;
  private ByteBuffer processorBuffer = ByteBuffer.allocate((int) firstLayerBufferSize);

  private ILogWriter currentFileWriter;

  private RestorableTsFileIOWriter writer;
  private final boolean sequence;


  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private ConcurrentLinkedQueue flushTaskQueue;
  private Future flushTaskFuture;
  private boolean noMoreIndexFlushTask = false;

  public IndexFileProcessor(String storageGroupName, String indexParentDir, String indexFullPath,
      boolean sequence) {
    this.storageGroupName = storageGroupName;
    this.indexParentDir = indexParentDir;
    this.indexFullPath = indexFullPath;
    this.sequence = sequence;

    indexMap = new EnumMap<>(IndexType.class);
  }

  public void flush(PhysicalPlan plan) throws IOException {
    lock.writeLock().lock();
    try {
      putLog(plan);
//      if (bufferedLogNum >= config.getFlushWalThreshold()) {
//        sync();
//      }
    } catch (BufferOverflowException e) {
      throw new IOException("Log cannot fit into buffer, please increase wal_buffer_size", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void putLog(PhysicalPlan plan) {
    processorBuffer.mark();
    try {
      plan.serializeTo(processorBuffer);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      processorBuffer.reset();
//      sync();
      plan.serializeTo(processorBuffer);
    }

  }

  /**
   * seal the index file, move "indexing" to "index"
   */
  public void close() {
//    sync();
//    forceWal();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", indexFullPath);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", indexFullPath, e);
    } finally {
      lock.writeLock().unlock();
    }
    String indexedFullPath = indexFullPath.substring(0, indexFullPath.length() -
        INDEXING_SUFFIX.length()) + INDEXED_SUFFIX;
    File oldFile = FSFactoryProducer.getFSFactory().getFile(indexFullPath);
    File newFile = FSFactoryProducer.getFSFactory().getFile(indexedFullPath);
    if (oldFile.renameTo(newFile)) {
      logger.error("Renaming indexing file to indexed failed {}.", indexedFullPath);
    }
  }

  public void forceSync() {
//    sync();
//    forceWal();
  }

//  public void notifyStartFlush() {
//    lock.writeLock().lock();
//    try {
//      close();
//      nextFileWriter();
//    } finally {
//      lock.writeLock().unlock();
//    }
//  }
//
//  public void notifyEndFlush() {
//    lock.writeLock().lock();
//    try {
//      File logFile = SystemFileFactory.INSTANCE.getFile(indexGroupDirectory, INDEX_FILE_NAME + ++lastFlushedId);
//      discard(logFile);
//    } finally {
//      lock.writeLock().unlock();
//    }
//  }

  public String getIndexFullPath() {
    return indexFullPath;
  }

  public String getIndexParentDir() {
    return indexParentDir;
  }

//  private ILogWriter getCurrentFileWriter() {
//    if (currentFileWriter == null) {
//      nextFileWriter();
//    }
//    return currentFileWriter;
//  }

//  private void nextFileWriter() {
//    fileId++;
//    File newFile = SystemFileFactory.INSTANCE.getFile(indexGroupDirectory, INDEX_FILE_NAME + fileId);
//    if (newFile.getParentFile().mkdirs()) {
//      logger.info("create WAL parent folder {}.", newFile.getParent());
//    }
//    currentFileWriter = new LogWriter(newFile);
//  }


  @Override
  public int hashCode() {
    return indexFullPath.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((IndexFileProcessor) obj) == 0;
  }

  @Override
  public String toString() {
    return "Index File: " + indexFullPath;
  }

  @Override
  public int compareTo(IndexFileProcessor o) {
    return indexFullPath.compareTo(o.indexFullPath);
  }

  public void startFlushMemTable() {
    this.flushTaskQueue = new ConcurrentLinkedQueue();
    this.flushTaskFuture = subTaskPoolManager.submit(flushRunTask);
    this.noMoreIndexFlushTask = false;
  }


  @SuppressWarnings("squid:S135")
  private Runnable flushRunTask = () -> {
    long ioTime = 0;
    boolean returnWhenNoTask = false;
    while (true) {
      if (noMoreIndexFlushTask) {
        returnWhenNoTask = true;
      }
      Object indexFlushMessage = flushTaskQueue.poll();
      if (indexFlushMessage == null) {
        if (returnWhenNoTask) {
          break;
        }
        try {
          Thread.sleep(10);
        } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
          logger.error("Index Flush Task is interrupted, index path {}", indexFullPath, e);
          break;
        }
      } else {
        try {
          // TODO flush task
          if (indexFlushMessage instanceof StartFlushGroupIOTask) {
            writer.startChunkGroup(((StartFlushGroupIOTask) indexFlushMessage).deviceId);
          } else if (indexFlushMessage instanceof IChunkWriter) {
            ChunkWriterImpl chunkWriter = (ChunkWriterImpl) indexFlushMessage;
            chunkWriter.writeToFileWriter(MemTableFlushTask.this.writer);
          } else {
            writer.endChunkGroup();
          }
        } catch (@SuppressWarnings("squid:S2139") IOException e) {
          logger.error("Index Flush Task meet IO error, index path: {}", indexFullPath, e);
          throw new FlushRunTimeException(e);
        }
      }
    }
  };

  public void buildIndexForOneSeries(Path path, TVList tvList) {
    tvList
    CopyOnReadLinkedList<T>
//    TODO
  }


  public void endFlushMemTable() {
    noMoreIOTask = true;
  }
}
