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
package org.apache.iotdb.db.utils.datastructure.primitive;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

import org.junit.Test;

import static org.junit.Assert.*;

public class BooleanPrimitiveListTest {

  @Test
  public void setBoolean() throws IllegalPathException {
    QueryIndexPlan plan = new QueryIndexPlan();
    //    System.out.println(plan instanceof QueryIndexPlan);
    System.out.println(plan instanceof QueryPlan);
    //    PartialPath p = new PartialPath("asd.asd.ew.17238192");
    //    System.out.println(Arrays.toString(p.getNodes()));
    //    PrimitiveList booleanList = PrimitiveList.newList(TSDataType.BOOLEAN);
    //    System.out.println(String.format("%b,", true));
    //    System.out.println(String.format("%b,", false));
    //    booleanList.setBoolean(0, true);
    //    booleanList.setBoolean(1, true);
    //    booleanList.setBoolean(8, true);
    //    Assert.assertEquals("{true,true,false,false,false,false,false,false,true,}",
    // booleanList.toString());
    //    System.out.println(booleanList);
  }
}
