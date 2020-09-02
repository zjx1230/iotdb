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
package org.apache.iotdb.db.qp.plan;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.DropIndexPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * refer to org.apache.iotdb.db.qp.plan.PhysicalPlanTest
 */
public class IndexPhysicalPlanTest {

  private Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    MManager.getInstance().init();
    MManager.getInstance().setStorageGroup(new PartialPath("root.vehicle"));
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d1.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d1.s2"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d2.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
  }

  @After
  public void clean() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }


  @Test
  public void aaa() {
    int a = 1;
    System.out.println(++a);
    Map<IndexType, IndexInfo> indexInfoMaps = new EnumMap<>(IndexType.class);
    Map<String, String> aa = new HashMap<>();
    aa.put("asd", "asd");
    aa.put("asd2", "frd");
    aa.put("asd4", "zxc");
    indexInfoMaps.put(IndexType.PAA, new IndexInfo(IndexType.PAA, 1, aa));
    indexInfoMaps.put(IndexType.ELB, new IndexInfo(IndexType.ELB, 2, aa));
    indexInfoMaps.put(IndexType.KV_INDEX, new IndexInfo(IndexType.KV_INDEX, 2, aa));
    Collection<IndexInfo> rr = indexInfoMaps.values();
    List<IndexInfo> r = new ArrayList<>(rr);
    System.out.println(r);

  }

  @Test
  public void testCreateIndex() throws QueryProcessException {
    String sqlStr = "CREATE INDEX ON root.vehicle.d1.s1 WHERE time > 50 WITH INDEX=PAA, window_length=100, merge_threshold= 0.5";

    Planner processor = new Planner();
    CreateIndexPlan plan = (CreateIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(
        "paths: [root.vehicle.d1.s1], index type: PAA, start time: 50, props: {WINDOW_LENGTH=100, MERGE_THRESHOLD=0.5}",
        plan.toString());
  }

  @Test
  public void testDropIndex() throws QueryProcessException {
    String sqlStr = "DROP INDEX PAA ON root.vehicle.d1.s1";
    Planner processor = new Planner();
    DropIndexPlan plan = (DropIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals("paths: [root.vehicle.d1.s1], index type: PAA", plan.toString());
  }

  @Test
  public void testQueryIndex() throws QueryProcessException {
    String sqlStr = "select index whole_st_time(s1), dist(s2) from root.vehicle.d1 where "
        + "time <= 51 or !(time != 100 and time < 460) WITH INDEX=PAA, threshold=5, distance=DTW";
    QueryIndexPlan plan = (QueryIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    Assert.assertEquals(
        "Aggregation info: Paths: [root.vehicle.d1.s1, root.vehicle.d1.s2], agg names: [whole_st_time, dist], data types: [FLOAT, FLOAT], filter: [((time <= 51 || time == 100) || time >= 460)], index type: PAA, props: {THRESHOLD=5, DISTANCE=DTW}",
        plan.toString());
  }

}
