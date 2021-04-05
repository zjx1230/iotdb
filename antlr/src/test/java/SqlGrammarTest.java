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

import org.apache.iotdb.db.sql.parser.SQLParseUtil;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class SqlGrammarTest {

  private String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      // 33, 128
      builder.append((char) (ThreadLocalRandom.current().nextInt(65, 90)));
    }
    return builder.toString();
  }

  @Test
  public void testPerforanceForTree() {
    String sqlFormat = "select %s from root.%s where time > %d";
    StringBuilder select = new StringBuilder(200);
    StringBuilder from = new StringBuilder(210);
    String[] sqls = new String[100_000 + 1];

    for (int i = 0; i < sqls.length; i++) {
      // generate select
      for (int j = 0; j < 19; j++) {
        select.append(randomString(9)).append(",");
      }
      select.append(randomString(10));
      // generate from
      for (int j = 0; j < 4; j++) {
        for (int k = 0; k < 4; k++) {
          from.append(randomString(9)).append(".");
        }
        from.append(randomString(9)).append(", root.");
      }
      from.append(randomString(6));
      sqls[i] = String.format(sqlFormat, select, from, System.currentTimeMillis());
      select = new StringBuilder(200);
      from = new StringBuilder(210);
    }
    sqls[sqls.length - 1] =
        "select select, create, *, abc, abc* from root.select.int32, root.int64";
    long time = System.currentTimeMillis();
    for (String sql : sqls) {
      try {
        SQLParseUtil.parseTree(sql);
      } catch (Exception e) {
        System.out.println(sql);
        e.printStackTrace();
      }
    }
    System.out.println("time cost (ms) : " + (System.currentTimeMillis() - time));
  }
}
