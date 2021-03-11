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

import java.util.Random;

/** Created by kangrong on 17/1/2. */
public class GaussProba extends ProbUtil {
  private final double sigma;
  private final double miu;
  private final double sqrtSigma;
  private Random r;

  public GaussProba(double sigma, double miu) {
    this.sigma = sigma;
    this.sqrtSigma = Math.sqrt(sigma);
    this.miu = miu;
    this.r = new Random();
  }

  @Override
  public double getRangeProbability(double up, double down) {
    assert up > down;
    if (up == Double.MAX_VALUE && down == -Double.MAX_VALUE) return 1;
    else if (up == Double.MAX_VALUE) {
      double normDown = (down - miu) / sigma;
      double downProba = (normDown > 0 ? normsDist(normDown) : 1 - normsDist(0 - normDown));
      return 1 - downProba;
    } else if (down == -Double.MAX_VALUE) {
      double normUp = (up - miu) / sigma;
      double upProba = (normUp > 0 ? normsDist(normUp) : 1 - normsDist(0 - normUp));
      return upProba;
    } else {
      double normUp = (up - miu) / sigma;
      double upProba = (normUp > 0 ? normsDist(normUp) : 1 - normsDist(0 - normUp));
      double normDown = (down - miu) / sigma;
      double downProba = (normDown > 0 ? normsDist(normDown) : 1 - normsDist(0 - normDown));
      return upProba - downProba;
    }
  }

  public static double getRangeProbability(double sigma, double miu, double up, double down) {
    assert up > down;
    double normUp = (up - miu) / sigma;
    double upProba = (normUp > 0 ? normsDist(normUp) : 1 - normsDist(0 - normUp));
    double normDown = (down - miu) / sigma;
    double downProba = (normDown > 0 ? normsDist(normDown) : 1 - normsDist(0 - normDown));
    return upProba - downProba;
  }

  @Override
  public double getNextRandom() {
    return r.nextGaussian() * sigma + miu;
  }

  protected static double normsDist(double a) {
    double p = 0.2316419;
    double b1 = 0.31938153;
    double b2 = -0.356563782;
    double b3 = 1.781477937;
    double b4 = -1.821255978;
    double b5 = 1.330274429;

    double x = Math.abs(a);
    double t = 1 / (1 + p * x);

    double val =
        1
            - (1 / (Math.sqrt(2 * Math.PI)) * Math.exp(-1 * Math.pow(a, 2) / 2))
                * (b1 * t
                    + b2 * Math.pow(t, 2)
                    + b3 * Math.pow(t, 3)
                    + b4 * Math.pow(t, 4)
                    + b5 * Math.pow(t, 5));

    if (a < 0) {
      val = 1 - val;
    }

    return val;
  }
}
