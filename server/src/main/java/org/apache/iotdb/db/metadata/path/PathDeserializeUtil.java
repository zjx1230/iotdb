/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.tsfile.read.common.Path;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class PathDeserializeUtil {

  public static Path deserialize(ByteBuffer buffer) {
    byte pathType = buffer.get();
    switch (pathType) {
      case 0:
        return MeasurementPath.deserialize(buffer);
      case 1:
        return AlignedPath.deserialize(buffer);
      case 2:
        return PartialPath.deserialize(buffer);
      case 3:
        return Path.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Invalid path type: " + pathType);
    }
  }
}

enum PathType {
  Measurement((byte) 0),
  Aligned((byte) 1),
  Partial((byte) 2),
  Path((byte) 3);

  private final byte pathType;

  PathType(byte pathType) {
    this.pathType = pathType;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.put(pathType);
  }
}
