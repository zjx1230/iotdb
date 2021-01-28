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
package org.apache.iotdb.db.index.common.distance;

import static org.apache.iotdb.db.index.common.IndexUtils.getDoubleFromAnyType;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * L∞-norm Euclidean distance
 */
public class LInfinityNormdouble implements Distance {


  @Override
  public double distWithoutSqrt(double a, double b) {
    return Math.abs(a - b);
  }

  @Override
  public double dist(double[] a, double[] b) {
    double max = 0;
    for (int i = 0; i < a.length; i++) {
      double dis = Math.abs(a[i] - b[i]);
        if (dis > max) {
            max = dis;
        }
    }
    return max;
  }

  @Override
  public double dist(double[] a, int aOffset, TVList b, int bOffset, int length) {
    assert a.length >= aOffset + length && a.length >= bOffset + length;
    double max = 0;
    for (int i = 0; i < length; i++) {
      double dis = Math.abs(a[i + aOffset] - getDoubleFromAnyType(b, i + bOffset));
        if (dis > max) {
            max = dis;
        }
    }
    return max;
  }

  @Override
  public double distPower(double[] a, int aOffset, TVList b, int bOffset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double distPower(double[] a, int aOffset, double[] b, int bOffset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double distPower(TVList a, int aOffset, double[] b, int bOffset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length,
      double threshold) {
    assert a.length >= aOffset + length && a.length >= bOffset + length;
    for (int i = length - 1; i >= 0; i--) {
      double dis = Math.abs(a[i + aOffset] - b[i + bOffset]);
        if (dis > threshold) {
            return i;
        }
    }
    return -1;
  }

  @Override
  public int distEarlyAbandonDetail(double[] a, int aOffset, double[] b, int bOffset, int length,
      double threshold) {
    assert a.length >= aOffset + length && a.length >= bOffset + length;
    for (int i = 0; i < length; i++) {
      double dis = Math.abs(a[i + aOffset] - b[i + bOffset]);
        if (dis > threshold) {
            return (i + 1);
        }
    }
    return -length;
  }

  @Override
  public double getThresholdNoRoot(double threshold) {
    return threshold;
  }

  @Override
  public double getP() {
    return Double.MAX_VALUE;
  }

  @Override
  public int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, TVList b, int bOffset,
      int length, double thresPow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, double[] b, int bOffset,
      int length, double thresholdPow) {
    return 0;
  }

  @Override
  public String toString() {
    return "LNorm_Inf";
  }
}


