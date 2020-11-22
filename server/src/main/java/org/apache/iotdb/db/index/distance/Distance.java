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
package org.apache.iotdb.db.index.distance;

import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;

import org.apache.iotdb.db.index.common.IndexConstant;
import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * Measure distance between two doubles or two series.
 */
public interface Distance {

  double distWithoutSqrt(double a, double b);

  double dist(double[] a, double[] b);

  double dist(double[] a, int aOffset, TVList b, int bOffset, int length);

  double distPower(double[] a, int aOffset, TVList b, int bOffset, int length);

  double distPower(double[] a, int aOffset, double[] b, int bOffset, int length);

  double distPower(TVList a, int aOffset, double[] b, int bOffset, int length);

  int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length, double thres);

  int distEarlyAbandonDetail(double[] a, int aOffset, double[] b, int bOffset, int length,
      double threshold);

  double getThresholdNoRoot(double threshold);

  double getP();

  int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, TVList b, int bOffset, int length,
      double thresholdPow);

  int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, double[] b, int bOffset, int length,
      double thresholdPow);

  static Distance getDistance(String distance) {
    if (L_INFINITY.equals(distance)) {
      return new LInfinityNormdouble();
    } else {
      return new LNormDouble(Integer.parseInt(distance));
    }
  }
}
