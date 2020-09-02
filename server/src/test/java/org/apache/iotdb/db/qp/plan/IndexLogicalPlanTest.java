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

import java.util.Arrays;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.QueryIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.DropIndexOperator;
import org.apache.iotdb.db.qp.strategy.ParseDriver;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexLogicalPlanTest {

  private ParseDriver parseDriver;
  private Planner processor = new Planner();

  @Before
  public void before() {
    parseDriver = new ParseDriver();
  }

  @Test
  public void testParseCreateIndex() {
    String sqlStr = "CREATE INDEX ON root.vehicle.d1.s1 WHERE time > 50 WITH INDEX=PAA, WINDOW_LENGTH=100, merge_threshold= 0.5";
    Operator op = parseDriver.parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(CreateIndexOperator.class, op.getClass());
    CreateIndexOperator createOperator = (CreateIndexOperator) op;
    Assert.assertEquals(OperatorType.CREATE_INDEX, createOperator.getType());
    Assert.assertNotNull(createOperator.getSelectedPaths());
    Assert.assertEquals("root.vehicle.d1.s1", createOperator.getSelectedPaths().get(0).toString());
    Assert.assertNull(createOperator.getFromOperator());
    Assert.assertEquals(IndexType.PAA_INDEX, createOperator.getIndexType());
    Assert.assertEquals(50, createOperator.getTime());
    Assert.assertEquals(2, createOperator.getProps().size());
    Assert.assertEquals("100", createOperator.getProps().get("WINDOW_LENGTH"));
    Assert.assertEquals("0.5", createOperator.getProps().get("MERGE_THRESHOLD"));

  }

  @Test
  public void testParseDropIndex() {
    String sqlStr = "DROP INDEX PAA ON root.vehicle.d1.s1";
    Operator op = parseDriver.parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(DropIndexOperator.class, op.getClass());
    DropIndexOperator dropIndexOperator = (DropIndexOperator) op;
    Assert.assertEquals(OperatorType.DROP_INDEX, dropIndexOperator.getType());
    Assert.assertNotNull(dropIndexOperator.getSelectedPaths());
    Assert.assertEquals("root.vehicle.d1.s1", dropIndexOperator.getSelectedPaths().get(0).toString());
    Assert.assertNull(dropIndexOperator.getFromOperator());
    Assert.assertEquals(IndexType.PAA_INDEX, dropIndexOperator.getIndexType());
  }

  @Test
  public void testParseQueryIndex() {
    String sqlStr = "select index whole_st_time(s1), dist(s2) from root.vehicle.d1 "
        + "where time <= 51 or !(time != 100 and time < 460) WITH INDEX=PAA, threshold=5, distance=DTW";
    Operator op = parseDriver.parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryIndexOperator.class, op.getClass());
    QueryIndexOperator queryOperator = (QueryIndexOperator) op;
    Assert.assertEquals(OperatorType.QUERY_INDEX, queryOperator.getType());
    Assert.assertTrue(queryOperator.hasAggregation());
    Assert.assertEquals(Arrays.asList("whole_st_time", "dist"),
        queryOperator.getSelectOperator().getAggregations());
    Assert.assertEquals("[or [time<=51][not [and [time<=>100][time<460]]]]",
        queryOperator.getFilterOperator().toString());
    Assert.assertEquals(Arrays.asList(new Path("s1"), new Path("s2")),
        queryOperator.getSelectedPaths());
    Assert.assertEquals("root.vehicle.d1",
        queryOperator.getFromOperator().getPrefixPaths().get(0).getFullPath());

    Assert.assertEquals(IndexType.PAA_INDEX, queryOperator.getIndexType());
    Assert.assertEquals(2, queryOperator.getProps().size());
    Assert.assertEquals("5", queryOperator.getProps().get("threshold".toUpperCase()));
    Assert.assertEquals("dtw".toUpperCase(), queryOperator.getProps().get("distance".toUpperCase()));
  }


}
