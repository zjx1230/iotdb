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

  int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length, double thres);

  int distEarlyAbandonDetail(double[] a, int aOffset, double[] b, int bOffset, int length,
      double threshold);

  double getThresholdNoRoot(double threshold);

  double getP();

  int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, TVList b, int bOffset, int length,
      double thresholdPow);

  static Distance getDistance(String distance) {
    if (L_INFINITY.equals(distance)) {
      return new LInfinityNormdouble();
    } else {
      return new LNormDouble(Integer.parseInt(distance));
    }
  }
}
