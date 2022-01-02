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
package org.apache.iotdb.tsfile.read.query.dataset;

import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * query processing: (1) generate time by series that has filter (2) get value of series that does
 * not have filter (3) construct RowRecord.
 */
public class DataSetWithTimeGenerator extends QueryDataSet {

  private static final Logger logger = LoggerFactory.getLogger(DataSetWithTimeGenerator.class);

  public static final AtomicBoolean generate = new AtomicBoolean(true);
  private static final AtomicLong classId = new AtomicLong(0);

  private final RecordGenerator generator;
  private TimeGenerator timeGenerator;
  private List<FileSeriesReaderByTimestamp> readers;
  private List<Boolean> cached;

  public static interface RecordGenerator {

    public void accept(RowRecord record, long timestamp);

  }

  /**
   * constructor of DataSetWithTimeGenerator.
   *
   * @param paths         paths in List structure
   * @param cached        cached boolean in List(boolean) structure
   * @param dataTypes     TSDataTypes in List structure
   * @param timeGenerator TimeGenerator object
   * @param readers       readers in List(FileSeriesReaderByTimestamp) structure
   */
  public DataSetWithTimeGenerator(
      List<Path> paths,
      List<Boolean> cached,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<FileSeriesReaderByTimestamp> readers) {
    super(paths, dataTypes);
    this.cached = cached;
    this.timeGenerator = timeGenerator;
    this.readers = readers;

    if (DataSetWithTimeGenerator.generate.get()) {
      // Do fancy stuff here, code was
//    FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp = readers.get(i);
//    Object value = fileSeriesReaderByTimestamp.getValueInTimestamp(timestamp);
//    if (dataTypes.get(i) == TSDataType.VECTOR) {
//      TsPrimitiveType v = ((TsPrimitiveType[]) value)[0];
//      rowRecord.addField(v.getValue(), v.getDataType());
//    } else {
//      rowRecord.addField(value, dataTypes.get(i));
//    }
      ParameterExpression readers_ = Expressions.parameter(new Types.ArrayType(FileSeriesReaderByTimestamp.class), "readers");
      ParameterExpression rowRecord_ = Expressions.parameter(RowRecord.class, "rowRecord");
      ParameterExpression timestamp_ = Expressions.parameter(long.class, "timestamp");


      ArrayList<Statement> body = new ArrayList<>();
      Method getValueInTimestamp;
      Method addField;
      Method getValue;
      Method getDataType;
      try {
        getValueInTimestamp = FileSeriesReaderByTimestamp.class.getMethod("getValueInTimestamp", long.class);
        addField = RowRecord.class.getMethod("addField", Field.class);
        getValue = TsPrimitiveType.class.getMethod("getValue");
        getDataType = TsPrimitiveType.class.getMethod("getDataType");
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException();
      }

      ParameterExpression reader_ = Expressions.parameter(FileSeriesReaderByTimestamp.class, "reader");
      ParameterExpression value_ = Expressions.parameter(Object.class, "value");

      body.add(Expressions.declare(0, reader_, null));
      body.add(Expressions.declare(0, value_, null));

      for (int i = 0; i < paths.size(); i++) {
        if (this.cached.get(i) == false) {
          body.add(
              Expressions.statement(
                  Expressions.assign(reader_,
                      Expressions.convert_(
                          Expressions.arrayIndex(readers_, Expressions.constant(i)),
                          FileSeriesReaderByTimestamp.class
                      )
                  )
              )
          );
          body.add(
              Expressions.statement(
                  Expressions.assign(value_, Expressions.call(reader_, getValueInTimestamp, timestamp_))
              )
          );
          if (dataTypes.get(i) == TSDataType.VECTOR) {
            ParameterExpression v_ = Expressions.parameter(TsPrimitiveType.class, "v");
            body.add(
                Expressions.declare(1, v_,
                    Expressions.convert_(
                        Expressions.arrayIndex(v_, Expressions.constant(0)),
                        TsPrimitiveType.class
                    )
                )
            );
            body.add(
                Expressions.statement(
                    Expressions.call(
                        rowRecord_, addField,
                        Expressions.call(v_, getValue),
                        Expressions.call(v_, getDataType)
                    )
                )
            );
          } else {
            // We can get rid of the switch statement here in addField
            ParameterExpression field_ = Expressions.parameter(Field.class, "field" + i);

            // From Field.getField(xxx)
            // TODO add Null Check
//            if (value == null) {
//              return null;
//            }


//            Field field = new Field(dataType);
            body.add(
                Expressions.declare(0, field_, Expressions.new_(Field.class, Expressions.constant(dataTypes.get(i))))
            );
            switch (dataTypes.get(i)) {
              case INT32:
                // field.setIntV((int) value);
                body.add(
                    Expressions.statement(
                        Expressions.call(field_, "setIntV",
                            Expressions.unbox(
                            Expressions.convert_(value_, Integer.class)
                            )
                        )
                    )
                );
                break;
//              case INT64:
//                field.setLongV((long) value);
//                break;
              case FLOAT:
//                field.setFloatV((float) value);
                body.add(
                    Expressions.statement(
                        Expressions.call(field_, "setFloatV",
                            Expressions.unbox(
                            Expressions.convert_(value_, Float.class)
                            )
                        )
                    )
                );
                break;
//              case DOUBLE:
//                field.setDoubleV((double) value);
//                break;
//              case BOOLEAN:
//                field.setBoolV((boolean) value);
//                break;
//              case TEXT:
//                field.setBinaryV((Binary) value);
//                break;
              default:
                throw new UnSupportedDataTypeException(dataTypes.get(i).toString());
            }


            // Old ("inefficient") method
//            body.add(
//                Expressions.statement(
//                    Expressions.call(
//                        rowRecord_, addField,
//                        value_,
//                        Expressions.constant(dataTypes.get(i))
//                    )
//                )
//            );

            // Efficient method as we avoid a switch in the Field::addField method
            body.add(
                Expressions.statement(
                    Expressions.call(
                        rowRecord_, addField,
                        field_
                    )
                )
            );
          }
        } else {
          throw new NotImplementedException();
        }
      }

      BlockStatement block = Expressions.block(
          Expressions.tryCatch(
              Expressions.block(body),
              Expressions.catch_(
                  Expressions.parameter(IOException.class, "e"),
                  Expressions.block()
              )
          )
      );

      MethodDeclaration method = Expressions.methodDecl(1, void.class, "accept", Arrays.asList(rowRecord_, timestamp_), block);

      String s = Expressions.toString(method);

      String className = "Generator" + classId.getAndIncrement();

      String s2 = "import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;\n" +
          "import java.util.List;" +
          "\nprivate final FileSeriesReaderByTimestamp[] readers;\n" +
          "public " + className + "(List<FileSeriesReaderByTimestamp> readers) {\nthis.readers = (FileSeriesReaderByTimestamp[])readers.toArray(new FileSeriesReaderByTimestamp[]{});\n}\n\n" + s;

      logger.debug(s2);

      RecordGenerator f;

      try {
        Scanner scanner = new Scanner("", new StringReader(s2));
        ClassBodyEvaluator ev = new ClassBodyEvaluator(scanner, className, null, new Class[]{RecordGenerator.class}, null);

        f = (RecordGenerator) ev.getClazz().getConstructors()[0].newInstance(readers);

      } catch (CompileException | IllegalAccessException | InstantiationException | InvocationTargetException | IOException ex) {
        ex.printStackTrace();
        throw new IllegalStateException();
      }

      this.generator = f;
    } else {
      this.generator = new RecordGenerator() {
        @Override
        public void accept(RowRecord record, long timestamp) {
          try {
            for (int i = 0; i < paths.size(); i++) {

              // get value from readers in time generator
              if (cached.get(i)) {
                Object value = null;

                value = timeGenerator.getValue(paths.get(i));

                if (dataTypes.get(i) == TSDataType.VECTOR) {
                  TsPrimitiveType v = ((TsPrimitiveType[]) value)[0];
                  record.addField(v.getValue(), v.getDataType());
                } else {
                  record.addField(value, dataTypes.get(i));
                }
                continue;
              }


              // get value from series reader without filter
              FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp = readers.get(i);
              Object value = fileSeriesReaderByTimestamp.getValueInTimestamp(timestamp);
              if (dataTypes.get(i) == TSDataType.VECTOR) {
                TsPrimitiveType v = ((TsPrimitiveType[]) value)[0];
                record.addField(v.getValue(), v.getDataType());
              } else {
                record.addField(value, dataTypes.get(i));
              }
            }
          } catch (IOException e) {
            throw new IllegalStateException();
          }
        }
      };
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    return timeGenerator.hasNext();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long timestamp = timeGenerator.next();
    RowRecord rowRecord = new RowRecord(timestamp);

    this.generator.accept(rowRecord, timestamp);

    return rowRecord;
  }
}
