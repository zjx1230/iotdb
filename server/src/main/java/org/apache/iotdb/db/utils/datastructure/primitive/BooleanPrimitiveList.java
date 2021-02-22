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

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class BooleanPrimitiveList extends PrimitiveList {

  private List<boolean[]> values;

  BooleanPrimitiveList() {
    super(TSDataType.BOOLEAN);
    values = new ArrayList<>();
  }

  @Override
  public void putBoolean(boolean value) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    values.get(arrayIndex)[elementIndex] = value;
    size++;
  }

  public void setBoolean(int i, boolean b) {
    while (i >= size) {
      checkExpansion();
      size++;
    }
    int arrayIndex = i / ARRAY_SIZE;
    int elementIndex = i % ARRAY_SIZE;
    values.get(arrayIndex)[elementIndex] = b;
  }

  @Override
  public boolean getBoolean(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  @Override
  void clearAndReleaseValues() {
    if (values != null) {
      for (boolean[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  protected void expandValues() {
    values.add((boolean[]) PrimitiveArrayManager.getPrimitiveArraysByType(TSDataType.BOOLEAN));
    capacity += ARRAY_SIZE;
  }

  @Override
  public BooleanPrimitiveList clone() {
    BooleanPrimitiveList cloneList = new BooleanPrimitiveList();
    cloneAs(cloneList);
    for (boolean[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private boolean[] cloneValue(boolean[] array) {
    boolean[] cloneArray = new boolean[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }
}
