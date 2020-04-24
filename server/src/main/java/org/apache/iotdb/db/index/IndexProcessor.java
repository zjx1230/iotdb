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

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class IndexProcessor implements Comparable<IndexProcessor> {

  public static final String INDEX_FILE_NAME = "index";
  private static final Logger logger = LoggerFactory.getLogger(IndexProcessor.class);

  private String path;
  private String indexGroupDirectory;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private static long firstLayerBufferSize = IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize();

  private Map<IndexType, IIndex> indexMap;
  private ByteBuffer processorBuffer = ByteBuffer.allocate((int)firstLayerBufferSize);


  private ILogWriter currentFileWriter;
  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0;

  /**
   * constructor.
   *
   * @param storageGroupPath the Storage Group path
   */
  public IndexProcessor(String storageGroupPath) {
    this.path = storageGroupPath;
    this.indexGroupDirectory =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + this.path;
    if (SystemFileFactory.INSTANCE.getFile(indexGroupDirectory).mkdirs()) {
      logger.info("create the index folder {}", indexGroupDirectory);
    }
    indexMap = new EnumMap<>(IndexType.class);
  }

  public void write(PhysicalPlan plan) throws IOException {
    lock.writeLock().lock();
    try {
      putLog(plan);
      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
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
      sync();
      plan.serializeTo(processorBuffer);
    }
    bufferedLogNum ++;
  }

  public void close() {
    sync();
    forceWal();
    lock.writeLock().lock();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", path);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", path, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void forceSync() {
    sync();
    forceWal();
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

  public String getPath() {
    return path;
  }

  public String getIndexGroupDirectory() {
    return indexGroupDirectory;
  }

  public void delete() throws IOException {
    lock.writeLock().lock();
    try {
      processorBuffer.clear();
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(indexGroupDirectory));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(indexGroupDirectory).listFiles();
    Arrays.sort(logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(INDEX_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles);
  }

//  private void discard(File logFile) {
//    if (!logFile.exists()) {
//      logger.info("Log file does not exist");
//    } else {
//      try {
//        FileUtils.forceDelete(logFile);
//        logger.info("Log node {} cleaned old file", path);
//      } catch (IOException e) {
//        logger.error("Old log file {} of {} cannot be deleted", logFile.getName(), path, e);
//      }
//    }
//  }

  private void forceWal() {
    lock.writeLock().lock();
    try {
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.error("Log node {} force failed.", path, e);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void sync() {
    lock.writeLock().lock();
    try {
      if (bufferedLogNum == 0) {
        return;
      }
//      try {
////        getCurrentFileWriter().write(processorBuffer);
//      } catch (IOException e) {
//        logger.error("Log node {} sync failed, change system mode to read-only", path, e);
//        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
//        return;
//      }
      processorBuffer.clear();
      bufferedLogNum = 0;
      logger.debug("Log node {} ends sync.", path);
    } finally {
      lock.writeLock().unlock();
    }
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
    return path.hashCode();
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

    return compareTo((IndexProcessor) obj) == 0;
  }

  @Override
  public String toString() {
    return "Log node " + path;
  }

  @Override
  public int compareTo(IndexProcessor o) {
    return this.path.compareTo(o.path);
  }
}
