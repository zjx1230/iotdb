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
package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MultiThreadMemTableFlushTask implements IMemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadMemTableFlushTask.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
      FlushSubTaskPoolManager.getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // we have multiple thread to do the encoding Task.
  private Future<?>[] encodingTaskFutures;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  int threadSize = 10;

  private final LinkedBlockingQueue<Object>[] encodingTaskQueues;
  private LinkedBlockingQueue<Object>[] ioTaskQueues;

  private String storageGroup;

  private IMemTable memTable;

  private volatile long memSerializeTime = 0L;
  private volatile long ioTime = 0L;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   */
  public MultiThreadMemTableFlushTask(
      IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    if (threadSize > memTable.getChunkGroupNumber()) {
      threadSize = memTable.getChunkGroupNumber();
    }
    this.encodingTaskQueues = new LinkedBlockingQueue[threadSize];
    ioTaskQueues = new LinkedBlockingQueue[threadSize];
    for (int i = 0; i < threadSize; i++) {
      ioTaskQueues[i] =
          config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()
              ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
              : new LinkedBlockingQueue<>();
      encodingTaskQueues[i] = new LinkedBlockingQueue<>();
    }

    this.encodingTaskFutures = submitEncodingTasks();
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);
    LOGGER.debug(
        "flush task of Storage group {} memtable is created, flushing to file {}.",
        storageGroup,
        writer.getFile().getName());
  }

  /** the function for flushing memtable. */
  @Override
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    LOGGER.info(
        "The memTable size of SG {} is {}, the avg series points num in chunk is {}, total timeseries number is {}",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber(),
        memTable.getSeriesNumber());

    long estimatedTemporaryMemSize = 0L;
    if (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()) {
      estimatedTemporaryMemSize =
          memTable.memSize() / memTable.getSeriesNumber() * config.getIoTaskQueueSizeForFlushing();
      SystemInfo.getInstance().applyTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    long start = System.currentTimeMillis();
    long sortTime = 0;

    // for map do not use get(key) to iteratate
    int i = 0;
    for (Map.Entry<String, Map<String, IWritableMemChunk>> memTableEntry :
        memTable.getMemTableMap().entrySet()) {
      encodingTaskQueues[i].put(new StartFlushGroupIOTask(memTableEntry.getKey()));

      final Map<String, IWritableMemChunk> value = memTableEntry.getValue();
      for (Map.Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = iWritableMemChunkEntry.getValue();
        MeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTVListForFlush();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueues[i].put(new Pair<>(tvList, desc));
      }

      encodingTaskQueues[i].put(new EndChunkGroupIoTask());
      i = (i + 1) % threadSize;
    }
    // every encoding task queue has a taskEnd marker
    for (LinkedBlockingQueue encodingTaskQueue : encodingTaskQueues) {
      encodingTaskQueue.put(new TaskEnd());
    }
    LOGGER.debug(
        "Storage group {} memtable flushing into file {}: data sort time cost {} ms.",
        storageGroup,
        writer.getFile().getName(),
        sortTime);

    for (Future encodingTaskFuture : encodingTaskFutures) {
      try {
        encodingTaskFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        // any failed encoding task will rollback the whole task
        for (LinkedBlockingQueue encodingTaskQueue : encodingTaskQueues) {
          encodingTaskQueue.clear();
        }
        ioTaskFuture.cancel(true);
        throw e;
      }
    }

    ioTaskFuture.get();

    try {
      writer.writePlanIndices();
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (config.isEnableMemControl()) {
      if (estimatedTemporaryMemSize != 0) {
        SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
      }
      SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);
    }

    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup,
        memTable,
        System.currentTimeMillis() - start);
  }

  private Future[] submitEncodingTasks() {
    Future[] futures = new Future[threadSize];
    for (int i = 0; i < threadSize; i++) {
      futures[i] =
          SUB_TASK_POOL_MANAGER.submit(new EncodingTask(i, encodingTaskQueues[i], ioTaskQueues[i]));
    }
    return futures;
  }

  class EncodingTask implements Runnable {
    LinkedBlockingQueue<Object> encodingTaskQueue;
    LinkedBlockingQueue<Object> ioTaskQueue;
    int threadNumber;

    EncodingTask(
        int threadNumber,
        LinkedBlockingQueue<Object> encodingTaskQueue,
        LinkedBlockingQueue<Object> ioTaskQueue) {
      this.threadNumber = threadNumber;
      this.encodingTaskQueue = encodingTaskQueue;
      this.ioTaskQueue = ioTaskQueue;
    }

    private void writeOneSeries(
        TVList tvPairs, IChunkWriter seriesWriterImpl, TSDataType dataType) {
      for (int i = 0; i < tvPairs.size(); i++) {
        long time = tvPairs.getTime(i);

        // skip duplicated data
        if ((i + 1 < tvPairs.size() && (time == tvPairs.getTime(i + 1)))) {
          continue;
        }

        // store last point for SDT
        if (i + 1 == tvPairs.size()) {
          ((ChunkWriterImpl) seriesWriterImpl).setLastPoint(true);
        }

        switch (dataType) {
          case BOOLEAN:
            seriesWriterImpl.write(time, tvPairs.getBoolean(i));
            break;
          case INT32:
            seriesWriterImpl.write(time, tvPairs.getInt(i));
            break;
          case INT64:
            seriesWriterImpl.write(time, tvPairs.getLong(i));
            break;
          case FLOAT:
            seriesWriterImpl.write(time, tvPairs.getFloat(i));
            break;
          case DOUBLE:
            seriesWriterImpl.write(time, tvPairs.getDouble(i));
            break;
          case TEXT:
            seriesWriterImpl.write(time, tvPairs.getBinary(i));
            break;
          default:
            LOGGER.error("Storage group {} does not support data type: {}", storageGroup, dataType);
            break;
        }
      }
    }

    @SuppressWarnings("squid:S135")
    @Override
    public void run() {
      LOGGER.debug(
          "Storage group {} memtable flushing to file {} starts to encoding data (Thread #{}).",
          storageGroup,
          writer.getFile().getName(),
          threadNumber);
      while (true) {

        Object task = null;
        try {
          task = encodingTaskQueue.take();
        } catch (InterruptedException e1) {
          LOGGER.error(
              "Storage group {}, file {}, Take task from encodingTaskQueue Interrupted (Thread #{})",
              storageGroup,
              writer.getFile().getName(),
              threadNumber);
          Thread.currentThread().interrupt();
          break;
        }
        if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
          try {
            ioTaskQueue.put(task);
          } catch (
              @SuppressWarnings("squid:S2142")
              InterruptedException e) {
            LOGGER.error(
                "Storage group {} memtable flushing to file {}, encoding task is interrupted.",
                storageGroup,
                writer.getFile().getName(),
                e);
            // generally it is because the thread pool is shutdown so the task should be aborted
            break;
          }
        } else if (task instanceof TaskEnd) {
          break;
        } else {
          long starTime = System.currentTimeMillis();
          Pair<TVList, MeasurementSchema> encodingMessage = (Pair<TVList, MeasurementSchema>) task;
          IChunkWriter seriesWriter = new ChunkWriterImpl(encodingMessage.right);
          writeOneSeries(encodingMessage.left, seriesWriter, encodingMessage.right.getType());
          seriesWriter.sealCurrentPage();
          seriesWriter.clearPageWriter();
          try {
            ioTaskQueue.put(seriesWriter);
          } catch (InterruptedException e) {
            LOGGER.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
          }
          memSerializeTime += System.currentTimeMillis() - starTime;
        }
      }
      try {
        ioTaskQueue.put(new TaskEnd());
      } catch (InterruptedException e) {
        LOGGER.error("Put task into ioTaskQueue Interrupted");
        Thread.currentThread().interrupt();
      }

      LOGGER.debug(
          "Storage group {}, flushing memtable {} into disk: (Thread #{}) Encoding data cost "
              + "{} ms.",
          storageGroup,
          writer.getFile().getName(),
          threadNumber,
          memSerializeTime);
    }
  }

  @SuppressWarnings("squid:S135")
  private Runnable ioTask =
      () -> {
        LOGGER.debug(
            "Storage group {} memtable flushing to file {} start io.",
            storageGroup,
            writer.getFile().getName());
        int i = -1;
        // whether the IO task is writing a new ChunkGroup.
        // if true, then we can choose task from any queue.
        // otherwise, we can not change the queue.
        boolean isNew = true;
        byte[] finished = new byte[threadSize / Byte.SIZE + 1];
        for (int j = 0; j < finished.length; j++) {
          finished[j] = 0;
        }

        while (true) {
          Object ioMessage = null;
          try {
            if (isNew) {
              while (ioMessage == null) {
                // round robin strategy to get a task.
                i = (i + 1) % threadSize;
                if ((finished[i / Byte.SIZE] & BIT_UTIL[i % Byte.SIZE]) == 1) {
                  // means the queue is done
                  continue;
                }
                ioMessage = ioTaskQueues[i].poll(10, TimeUnit.MILLISECONDS);
              }
            } else {
              ioMessage = ioTaskQueues[i].take();
            }
          } catch (InterruptedException e1) {
            LOGGER.error("take task from ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
            break;
          }
          long starTime = System.currentTimeMillis();
          try {
            if (ioMessage instanceof StartFlushGroupIOTask) {
              isNew = false;
              this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
            } else if (ioMessage instanceof TaskEnd) {
              // queue i is finished
              finished[i / Byte.SIZE] |= BIT_UTIL[i % Byte.SIZE];
              // check whether if all queues are finished.
              int j;
              for (j = 0; j < threadSize / Byte.SIZE; j++) {
                if (finished[j] != 0XFF) {
                  // not finished.
                  break;
                }
              }
              if (j < threadSize / Byte.SIZE) {
                continue;
              }
              for (j = 0; j < threadSize % Byte.SIZE; j++) {
                if ((finished[threadSize / Byte.SIZE] & BIT_UTIL[j]) == 0) {
                  // not finished.
                  break;
                }
              }
              if (j < threadSize % Byte.SIZE) {
                continue;
              }
              // finished
              break;
            } else if (ioMessage instanceof IChunkWriter) {
              ChunkWriterImpl chunkWriter = (ChunkWriterImpl) ioMessage;
              chunkWriter.writeToFileWriter(this.writer);
            } else {
              this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
              this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
              this.writer.endChunkGroup();
              isNew = true;
            }
          } catch (IOException e) {
            LOGGER.error(
                "Storage group {} memtable {}, io task meets error.", storageGroup, memTable, e);
            throw new FlushRunTimeException(e);
          }
          ioTime += System.currentTimeMillis() - starTime;
        }
        LOGGER.debug(
            "flushing a memtable to file {} in storage group {}, io cost {}ms",
            writer.getFile().getName(),
            storageGroup,
            ioTime);
      };

  static class TaskEnd {

    TaskEnd() {}
  }

  static class EndChunkGroupIoTask {

    EndChunkGroupIoTask() {}
  }

  static class StartFlushGroupIOTask {

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }

  private static final byte[] BIT_UTIL = new byte[] {1, 2, 4, 8, 16, 32, 64, -128};
}
