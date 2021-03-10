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
package org.apache.iotdb.db.index.common.math.probability;

/** Created by kangrong on 17/1/2. */
public abstract class ProbUtil {

  public abstract double getRangeProbability(double up, double down);

  public abstract double getNextRandom();

  public static ProbUtil getNormal(ProbabilityType type, double max, double min) {
    switch (type) {
      case GAUSSION:
        return new GaussProba((max - min) / 6, (max + min) / 2);
      case UNIFORM:
        return new UniformProba(max, min);
      default:
        return new NoProba();
    }
  }

  public static double getPattern(
      ProbabilityType type, double pUp, double pDown, double max, double min) {
    switch (type) {
      case GAUSSION:
        return GaussProba.getRangeProbability((pUp - pDown) / 6, (pUp + pDown) / 2, max, min);
      case UNIFORM:
        return UniformProba.getRangeProbability(pUp, pDown, max, min);
      default:
        return 1;
    }
  }
}
