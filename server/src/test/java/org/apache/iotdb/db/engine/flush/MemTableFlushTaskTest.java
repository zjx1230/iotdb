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

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class MemTableFlushTaskTest {
  String filePath = "target/tsfile.tsfile";
  IMemTable memTable;
  RestorableTsFileIOWriter writer;
  String[] devices = {"root.sg.d1", "root.sg.d2", "root.sg.d3", "root.sg.d4", "root.sg.d5"};
  String[] sensors = {"s1", "s2", "s3"};
  Integer[] types = {
    (int) TSDataType.INT32.serialize(),
    (int) TSDataType.DOUBLE.serialize(),
    (int) TSDataType.TEXT.serialize()
  };
  MeasurementMNode[] nodes = {
    new MeasurementMNode(null, "s1", new MeasurementSchema("s1", TSDataType.INT32), null),
    new MeasurementMNode(null, "s2", new MeasurementSchema("s2", TSDataType.DOUBLE), null),
    new MeasurementMNode(null, "s3", new MeasurementSchema("s3", TSDataType.TEXT), null),
  };

  @Before
  public void setUp() throws IllegalPathException, IOException {
    memTable = new PrimitiveMemTable();
    for (int i = 0; i < devices.length; i++) {
      InsertTabletPlan plan =
          new InsertTabletPlan(new PartialPath(devices[i]), sensors, Arrays.asList(types));
      Object[] columns = new Object[3];
      columns[0] = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      columns[1] = new double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
      columns[2] =
          new Binary[] {
            new Binary("a"),
            new Binary("b"),
            new Binary("c"),
            new Binary("d"),
            new Binary("e"),
            new Binary("f"),
            new Binary("g"),
            new Binary("h"),
            new Binary("i"),
            new Binary("j")
          };
      plan.setColumns(columns);
      plan.setTimes(new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
      plan.setMeasurements(sensors);
      plan.setMeasurementMNodes(nodes);
      memTable.write(plan, 0, 10);
    }
    writer = new RestorableTsFileIOWriter(new File(filePath));
  }

  @After
  public void tear() throws IOException {
    if (writer != null) {
      // writer.close();
      try {
        Files.delete(writer.getFile().toPath());
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void test() throws ExecutionException, InterruptedException, IOException {
    IMemTableFlushTask task = new MultiThreadMemTableFlushTask(memTable, writer, "root.sg");
    task.syncFlushMemTable();
    System.out.println("end file.....");
    writer.endFile();
    writer.close();

    checkFile();
  }

  private void checkFile() throws IOException {
    try (ReadOnlyTsFile reader = new ReadOnlyTsFile(new TsFileSequenceReader(filePath)); ) {
      QueryExpression expression =
          QueryExpression.create(
              Arrays.asList(
                  new Path("root.sg.d1.s1", true),
                  new Path("root.sg.d1.s2", true),
                  new Path("root.sg.d1.s3", true),
                  new Path("root.sg.d2.s1", true),
                  new Path("root.sg.d2.s2", true),
                  new Path("root.sg.d2.s3", true),
                  new Path("root.sg.d4.s1", true),
                  new Path("root.sg.d4.s2", true),
                  new Path("root.sg.d4.s3", true)),
              null);
      QueryDataSet set = reader.query(expression);
      int time = 1;
      while (set.hasNext()) {
        RowRecord record = set.next();
        Assert.assertEquals(time, record.getTimestamp());
        Assert.assertEquals(time, record.getFields().get(0).getIntV());
        Assert.assertEquals(time, record.getFields().get(1).getDoubleV(), 0.001);
        Assert.assertEquals(
            "" + (char) ('a' + time - 1), record.getFields().get(2).getStringValue());
        time++;
      }
    }
  }
}
