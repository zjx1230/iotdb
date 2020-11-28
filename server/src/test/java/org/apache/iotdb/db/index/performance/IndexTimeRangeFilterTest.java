/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.performance;

import java.io.IOException;
import org.apache.iotdb.db.index.IndexTestUtils;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

public class IndexTimeRangeFilterTest {

  @Test
  public void test2() {
    System.out.println(IndexUtils.removeQuotation("\'asd\'"));
    System.out.println(IndexUtils.removeQuotation("\'asd"));
    System.out.println(IndexUtils.removeQuotation("\"asd\'"));
  }

  @Test
  public void testFilter() throws IOException {
//    TimeFilter.TimeGt timeGt = TimeFilter.gt(500);
//    TimeFilter.TimeLt timeLt = TimeFilter.lt(1000);
//    AndFilter a = FilterFactory.and(timeGt, timeLt);
    long minTime = 100;
    long maxTime = 200;
//    System.out.println(TimeFilter.gt(99L).satisfyStartEndTime(minTime, maxTime));

//    Filter time = FilterFactory.or(TimeFilter.gtEq(201L), FilterFactory.and(TimeFilter.ltEq(99L), TimeFilter.gt(90L)));
//
//    System.out.println(time.satisfyStartEndTime(minTime, maxTime));
    TVList src = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    for (int i = 0; i < 10; i++) {
      src.putInt(10 * i, i);
    }
    TVList res = IndexUtils.alignUniform(src, 10);
    System.out.println(IndexTestUtils.tvListToString(res));

    src = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    src.putInt(0, 0);
    src.putInt(5, 1);
    src.putInt(20, 2);
    src.putInt(21, 3);
    src.putInt(40, 4);
    src.putInt(44, 5);
    src.putInt(61, 6);
    src.putInt(71, 7);
    src.putInt(90, 8);
    res = IndexUtils.alignUniform(src, 10);
    System.out.println(IndexTestUtils.tvListToString(res));


  }

}
