package org.apache.iotdb.db.index.distance;

/**
 * Measure distance bytween two doubles or two series.
 */
public interface Distance {
    double distWithoutSqrt(double a, double b);

    double dist(double[] a, double[] b);

    double dist(double[] a, int aOffset, double[] b, int bOffset, int length);

    double distPower(double[] a, int aOffset, double[] b, int bOffset, int length);

    int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length, double thres);

    int distEarlyAbandonDetail(double[] a, int aOffset, double[] b, int bOffset, int length,
        double thres);

//    double distNoRoot(double up, double lowerLine);

    double getThresNoRoot(double thres);

    double getP();

    int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, double[] b, int bOffset, int length,
        double
            thresPow);
}
