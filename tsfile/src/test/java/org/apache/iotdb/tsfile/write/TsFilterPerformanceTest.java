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

package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.codegen.Generator;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@State(Scope.Benchmark)
public class TsFilterPerformanceTest {

  private static Logger logger = LoggerFactory.getLogger(TsFilterPerformanceTest.class);

  public static int runs = 1;

  TsFileWriter writer = null;
  String fileName = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
  boolean closed = false;

  String getFilename() {
    String filePath =
        String.format(
            "/tmp/root.sg1/0/0/",
            "root.sg1",
            0,
            0);
    String fileName =
        100
            + FilePathUtils.FILE_NAME_SEPARATOR
            + 1
            + "-0-0.tsfile";
    return filePath.concat(fileName);
  }

  /**
   * Benchmark                                     Mode  Cnt      Score     Error  Units
   * TsFilterPerformanceTest.getSimpleUnoptimized  avgt   15  10699,176 ± 511,309  ms/op
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getSimpleUnoptimized() throws IOException {
    List<Filter> filters = getSimpleFilters();
    runTests(filters, false);
  }

  /**
   * Benchmark                                   Mode  Cnt      Score     Error  Units
   * TsFilterPerformanceTest.getSimpleOptimized  avgt   15  11084,838 ± 331,142  ms/op
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getSimpleOptimized() throws IOException {
    List<Filter> filters = getSimpleFilters();
    runTests(filters, true);
  }


  /**
   * Benchmark                                       Mode  Cnt      Score     Error  Units
   * TsFilterPerformanceTest.getStandardUnoptimized  avgt   15  10345,496 ± 571,557  ms/op
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getStandardUnoptimized() throws IOException {
    List<Filter> filters = getStandardFilters();
    runTests(filters, false);
  }

  /**
   * Benchmark                                     Mode  Cnt     Score     Error  Units
   * TsFilterPerformanceTest.getStandardOptimized  avgt   15  9864,581 ± 591,591  ms/op
   * -> 4.6%
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getStandardOptimized() throws IOException {
    List<Filter> filters = getStandardFilters();
    runTests(filters, true);
  }

  /**
   * Benchmark                                      Mode  Cnt      Score     Error  Units
   * TsFilterPerformanceTest.getComplexUnoptimized  avgt   15  15053,562 ± 115,997  ms/op
   * -> 12.3%
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getComplexUnoptimized() throws IOException {
    List<Filter> filters = getComplexFilters();
    runTests(filters, false);
  }

  /**
   * Benchmark                                    Mode  Cnt      Score     Error  Units
   * TsFilterPerformanceTest.getComplexOptimized  avgt   15  13196,650 ± 477,061  ms/op
   * @throws IOException
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Fork(3)
  public void getComplexOptimized() throws IOException {
    List<Filter> filters = getComplexFilters();
    runTests(filters, true);
  }

  @Test
  public void generate() throws IOException, WriteProcessException {
    String filename = getFilename();

    System.out.println("Writing to " + filename);
    File f = new File(filename);
    if (!f.getParentFile().exists()) {
      Assert.assertTrue(f.getParentFile().mkdirs());
    }
    writer = new TsFileWriter(f);
    registerTimeseries();

    for (long t = 0; t <= 100_000_000; t++) {
      if (t % 100 == 0) {
        System.out.println(t);
      }
      // normal
      TSRecord record = new TSRecord(t, "d1");
      record.addTuple(new FloatDataPoint("s1", (float) (100.0 * Math.random())));
      record.addTuple(new IntDataPoint("s2", (int) (100.0 * Math.random())));
      writer.write(record);
    }

    writer.close();
  }


  /**
   * Without optimization: 10798.055595900001 ms
   * With: 9907.020887499999
   * Improvement ~ 8%
   */
  @Test
  public void readMany() throws IOException {
    List<List<Filter>> scenarios = Arrays.asList(
        getSimpleFilters(),
        getStandardFilters(),
        getComplexFilters()
    );

    ArrayList<Double> results = new ArrayList<>();

    for (int scenarioCount = 0; scenarioCount < scenarios.size(); scenarioCount++) {
      System.out.println("Starting Scenario " + scenarioCount);
      List<Filter> filter = scenarios.get(scenarioCount);

      // First, no optimizer
      double noOptimizer = runTests(filter, false);
      double optimizer = runTests(filter, true);
      double improvement = 100.0 * (1 - optimizer / noOptimizer);

      System.out.println("==========================");
      System.out.println("Scenario " + scenarioCount);
      System.out.println("==========================");
      System.out.printf("Duration (no optimizer): %.2f ms\n", noOptimizer);
      System.out.printf("Duration (optimizer): %.2f ms\n", optimizer);
      System.out.printf("Improvement: %.2f %%\n\n", improvement);

      results.add(improvement);
    }

    System.out.println("==========================");
    System.out.println("Final Result");
    System.out.println("==========================");
    for (Double result : results) {
      System.out.printf("Improvement: %.2f %%\n", result);
    }
    System.out.println("==========================");
  }

  private double runTests(List<Filter> filter, boolean optimizer) throws IOException {
    Generator.active.set(optimizer);
    long start = System.nanoTime();
    for (int i = 1; i <= runs; i++) {
      logger.info("Round " + i);
      read(filter);
    }
    long end = System.nanoTime();

    double durationMs = (end - start) / 1e6;

    return durationMs;
  }

  private List<Filter> getSimpleFilters() {
    Filter filter = TimeFilter.lt(50_000_000);
    Filter filter2 = ValueFilter.gt(50);
    Filter filter3 = TimeFilter.ltEq(50_000_000);

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  private List<Filter> getStandardFilters() {
    Filter filter = TimeFilter.lt(50_000_000);
    Filter filter2 = FilterFactory.and(ValueFilter.gt(25), ValueFilter.lt(75));
    Filter filter3 =
        FilterFactory.and(TimeFilter.gtEq(10_000_000), TimeFilter.ltEq(40_000_000));

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  private List<Filter> getComplexFilters() {
    Filter filter = FilterFactory.and(TimeFilter.gt(10_000_000), TimeFilter.lt(90_000_000));
    Filter filter2 = FilterFactory.or(FilterFactory.or(
            FilterFactory.and(ValueFilter.gt(15), ValueFilter.lt(30)),
            FilterFactory.and(ValueFilter.gt(45), ValueFilter.lt(60))
        ),
        FilterFactory.and(ValueFilter.gt(70), ValueFilter.lt(90))
    );
    Filter filter3 =
        FilterFactory.or(
            FilterFactory.and(TimeFilter.gtEq(10_000_000), TimeFilter.ltEq(20_000_000)),
            FilterFactory.and(TimeFilter.gtEq(30_000_000), TimeFilter.ltEq(40_000_000))
        );

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  public void read(List<Filter> filters) throws IOException {
    TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(getFilename());
    TsFileReader fileReader = new TsFileReader(fileSequenceReader);

    IExpression IExpression =
        BinaryExpression.or(
            BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1", "s1"), filters.get(0)),
                new SingleSeriesExpression(new Path("d1", "s2"), filters.get(1))),
            new GlobalTimeExpression(filters.get(2)));

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1"))
            .addSelectedPath(new Path("d1", "s2"))
            .setExpression(IExpression);

    fileSequenceReader.getAllDevices();

    logger.debug("Starting query...");
    QueryDataSet dataSet = fileReader.query(queryExpression);


    logger.debug("Query done, start iteration...");

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      count++;
    }

    logger.debug("Iterartion done, " + count + " points");
  }

  private void registerTimeseries() {
    // register nonAligned timeseries "d1.s1","d1.s2","d1.s3"
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new UnaryMeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new UnaryMeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals("given nonAligned timeseries d1.s1 has been registered.", e.getMessage());
    }
    try {
      List<UnaryMeasurementSchema> schemas = new ArrayList<>();
      schemas.add(
          new UnaryMeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      writer.registerAlignedTimeseries(new Path("d1"), schemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d1 has been registered for nonAligned timeseries.", e.getMessage());
    }
    List<UnaryMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(
        new UnaryMeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    schemas.add(
        new UnaryMeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    writer.registerTimeseries(new Path("d1"), schemas);

    // Register aligned timeseries "d2.s1" , "d2.s2", "d2.s3"
    try {
      List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries and should not be expanded.",
          e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d2"),
          new UnaryMeasurementSchema(
              "s5", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries.", e.getMessage());
    }

    /*try {
      for (int i = 2; i < 3; i++) {
        writer.registerTimeseries(
            new Path("d" + i, "s1"),
            new UnaryMeasurementSchema(
                "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }*/
  }

}
