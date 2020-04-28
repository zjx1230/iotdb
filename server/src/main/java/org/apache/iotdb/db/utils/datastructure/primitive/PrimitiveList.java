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

import static org.apache.iotdb.db.rescon.PrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class PrimitiveList {

  private static final String ERR_PRIMITIVE_DATATYPE_NOT_MATCH = "Primitive DataType not match";

  protected static final int SMALL_ARRAY_LENGTH = 32;

  protected int size;


  public PrimitiveList() {
    size = 0;
  }

  public int size() {
    return size;
  }


  public void putLong(long value) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public void putInt(int value) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public void putFloat(float value) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public void putDouble(double value) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  protected abstract void expandValues();

  public abstract PrimitiveList clone();

  protected abstract void release();

  public void delete(long upperBound) {
    throw new UnsupportedOperationException("PrimitiveList not support delete");
  }

  public void clear() {
    size = 0;
    clearValue();
  }

  abstract void clearValue();

  protected void cloneAs(PrimitiveList cloneList) {
    cloneList.size = size;
  }


  protected void checkExpansion() {
    if ((size % ARRAY_SIZE) == 0) {
      expandValues();
    }
  }

  public static PrimitiveList newList(TSDataType dataType) {
    switch (dataType) {
      case INT64:
        return new LongPrimitiveList();
      case TEXT:
      case FLOAT:
      case INT32:
      case DOUBLE:
      case BOOLEAN:
      default:
        return null;
    }
  }
}
