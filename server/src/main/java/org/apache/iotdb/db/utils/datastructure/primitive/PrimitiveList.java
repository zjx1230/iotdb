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

import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class PrimitiveList {

  private static final String ERR_PRIMITIVE_DATATYPE_NOT_MATCH = "Primitive DataType not match";

  protected static final int SMALL_ARRAY_LENGTH = 32;

  protected int size;
  protected final TSDataType tsDataType;
  protected int capacity;


  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public PrimitiveList(TSDataType tsDataType) {
    size = 0;
    capacity = 0;
    this.tsDataType = tsDataType;
  }

  public int size() {
    return size;
  }

  public int getCapacity() {
    return capacity;
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

  public void putAllDouble(PrimitiveList src) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }

  public boolean getBoolean(int index) {
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

  public void putBoolean(boolean value) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);

  }

  public void setBoolean(int i, boolean b) {
    throw new UnsupportedOperationException(ERR_PRIMITIVE_DATATYPE_NOT_MATCH);
  }


  protected abstract void expandValues();

  public abstract PrimitiveList clone();

  public void delete(long upperBound) {
    throw new UnsupportedOperationException("PrimitiveList not support delete");
  }

  public void clearButNotRelease() {
    size = 0;
  }

  public void clearAndRelease() {
    size = 0;
    capacity = 0;
    clearAndReleaseValues();
  }

  abstract void clearAndReleaseValues();

  protected void cloneAs(PrimitiveList cloneList) {
    cloneList.size = size;
  }


  protected void checkExpansion() {
    if (size == capacity) {
      expandValues();
    }
  }

  public static PrimitiveList newList(TSDataType dataType) {
    switch (dataType) {
      case INT64:
        return new LongPrimitiveList();
      case INT32:
        return new IntPrimitiveList();
      case FLOAT:
        return new FloatPrimitiveList();
      case DOUBLE:
        return new DoublePrimitiveList();
      case TEXT:
      case BOOLEAN:
      default:
        throw new NotImplementedException("unsupported type: " + dataType);
    }
  }


}
