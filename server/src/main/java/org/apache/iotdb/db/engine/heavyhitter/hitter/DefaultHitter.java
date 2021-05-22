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

package org.apache.iotdb.db.engine.heavyhitter.hitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * user defined hitter
 */
public class DefaultHitter implements QueryHeavyHitters {

  private static final Logger logger = LoggerFactory.getLogger(DefaultHitter.class);
  protected final ReadWriteLock hitterLock = new ReentrantReadWriteLock();

  public DefaultHitter(int maxHitterNum) {

  }

  @Override
  public void acceptQuerySeriesList(List<PartialPath> queryPaths) {
    hitterLock.writeLock().lock();
    try {
      for (PartialPath path : queryPaths) {
        acceptQuerySeries(path);
      }
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    // do nothing
  }

  /**
   *
   * @param sgName storage group name
   * @return top compaction series by sgName
   */
  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    int totalSG = StorageEngine.getInstance().getProcessorMap().size();
    List<PartialPath> ret = new ArrayList<>();
    List<PartialPath> unmergedSeries =
        MManager.getInstance().getAllTimeseriesPath(sgName);

//    Collections.shuffle(unmergedSeries);
    for (int i = 0; i < IoTDBDescriptor.getInstance().getConfig().getMaxHitterNum() / totalSG;
        i++) {
      ret.add(unmergedSeries.get(i));
    }
    logger.info("default hitter, compaction series example : {}", ret.get(0));
    return ret;
  }
}
