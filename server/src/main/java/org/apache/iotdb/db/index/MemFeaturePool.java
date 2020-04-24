/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.index;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 所有索引将原始数据预处理（不处理则为emptyProcess）之后的结果存入MEMFeature。
 * 如果不处理，则至少要做切片，
 * 因此，至少数据的形式是：
 * 原始数据 + 切片指针：int[] offset int[] length
 *
 * 如果做DTW，则可能是offset + 不等长的length（根据时间戳计算）
 *
 * 如果是ELB，则是 b * [upper, lower] (2 * float)
 *
 * 如果是
 *
 * 有mem监控，满了，或者全部处理完了，则进入索引构建阶段
 */
public class MemFeaturePool {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(MemFeaturePool.class);

  private static final Deque<IMemTable> availableMemTables = new ArrayDeque<>();
  private static final int WAIT_TIME = 2000;
  private int size = 0;

  private MemFeaturePool() {
  }

  public static MemFeaturePool getInstance() {
    return InstanceHolder.INSTANCE;
  }

  // TODO change the impl of getAvailableMemTable to non-blocking
  public IMemTable getAvailableMemTable(Object applier) {
    synchronized (availableMemTables) {
      if (availableMemTables.isEmpty() && size < CONFIG.getMaxMemtableNumber()) {
        size++;
        logger.info("generated a new memtable for {}, system memtable size: {}, stack size: {}",
            applier, size, availableMemTables.size());
        return new PrimitiveMemTable();
      } else if (!availableMemTables.isEmpty()) {
        logger
            .debug(
                "system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
                size, availableMemTables.size(), applier);
        return availableMemTables.pop();
      }

      // wait until some one has released a memtable
      int waitCount = 1;
      while (true) {
        if (!availableMemTables.isEmpty()) {
          logger.debug(
              "system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
              size, availableMemTables.size(), applier);
          return availableMemTables.pop();
        }
        try {
          availableMemTables.wait(WAIT_TIME);
        } catch (InterruptedException e) {
          logger.error("{} fails to wait fot memtables {}, continue to wait", applier, e);
          Thread.currentThread().interrupt();
        }
        logger.info("{} has waited for a memtable for {}ms", applier, waitCount++ * WAIT_TIME);
      }
    }
  }

  public void putBack(IMemTable memTable, String storageGroup) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    synchronized (availableMemTables) {
      // because of dynamic parameter adjust, the max number of memtable may decrease.
      if (size > CONFIG.getMaxMemtableNumber()) {
        logger.debug(
            "Currently the size of available MemTables is {}, the maxmin size of MemTables is {}, discard this MemTable.",
            CONFIG.getMaxMemtableNumber(), size);
        size--;
        return;
      }
      memTable.clear();
      availableMemTables.push(memTable);
      availableMemTables.notify();
      logger.debug("{} return a memtable, stack size {}", storageGroup, availableMemTables.size());
    }
  }

  public int getSize() {
    return size;
  }

  private static class InstanceHolder {

    private static final MemFeaturePool INSTANCE = new MemFeaturePool();

    private InstanceHolder() {
    }
  }
}
