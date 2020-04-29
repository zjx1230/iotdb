package org.apache.iotdb.db.index.distance;

/**
 * Euclidean distance, not including L∞-norm
 * Created by kangrong on 16/12/27.
 */
public class LNormdouble implements Distance{
    public final double getP() {
        return p;
    }

    private int p;

    public LNormdouble(int p) {
        assert p >= 1;
        this.p = p;
    }

    @Override
    public double distWithoutSqrt(double a, double b) {
        return pow(Math.abs(a-b));
    }

    public double pow(double r) {
        double s = r;
        for(int i=1; i<p; i++)
            s *= r;
        return s;
    }

    public static double pow(double r, int pp) {
        double s = r;
        for(int i=1; i<pp; i++)
            s *= r;
        return s;
    }
//    @Override
//    public double distNoRoot(double up, double lowerLine) {
//        return (double) Math.pow(Math.abs(up - lowerLine), p);
//    }

    @Override
    public double getThresNoRoot(double thres) {
        return pow(thres);
    }


    public double dist(double[] a, double[] b){
        assert a.length == b.length;
        double dis = 0;
        for (int i = 0; i < a.length; i++) {
//             dis += pow(Math.abs(a[i]-b[i]));
            dis += pow(Math.abs(a[i]-b[i]));
        }
        return Math.pow(dis,1.0/p);
    }

    public double dist(double[] a, int aOffset, double[] b, int bOffset, int length){
        double dis = distPower(a, aOffset, b, bOffset, length);
        return dis == 0 ? 0: Math.pow(dis,1.0/p);
    }

    public double distPower(double[] a, int aOffset, double[] b, int bOffset, int length){
        assert a.length >= aOffset+length && a.length >= bOffset+length;
        double dis = 0;
        for (int i = 0; i < length; i++) {
//            addDisSum++;
            dis += pow(Math.abs(a[i+aOffset]-b[i+bOffset]));
        }
        return dis;
    }

    @Override
    public int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length, double thres) {
        assert a.length >= aOffset + length && b.length >= bOffset + length;
        double thresPow = pow(thres);
        double dis = 0;
//        for (int i = length - 1; i >= 0; i--) {
//            dis += Math.pow(Math.abs(a[i + aOffset] - b[i + bOffset]), p);
//            if(dis > thresPow)
//                return i;
//        }
        for (int i = 0; i < length; i++) {
            dis += pow(Math.abs(a[i + aOffset] - b[i + bOffset]));
//            addEarlySum++;
            if(dis > thresPow)
                return i;
        }
        return -1;
    }

    @Override
    public int distEarlyAbandonDetail(double[] a, int aOffset, double[] b, int bOffset, int length, double thres) {
        assert a.length >= aOffset + length && b.length >= bOffset + length;
        double thresPow = pow(thres);
        double dis = 0;
        for (int i = 0; i < length; i++) {
            dis += pow(Math.abs(a[i + aOffset] - b[i + bOffset]));
            if(dis > thresPow)
                return (i+1);
        }
        return -length;
    }

    @Override
    public int distEarlyAbandonDetailNoRoot(double[] a, int aOffset, double[] b, int bOffset, int length, double
            thresPow) {
        assert a.length >= aOffset + length && b.length >= bOffset + length;
//        double thresPow = pow(thres);
        double dis = 0;
        for (int i = 0; i < length; i++) {
            dis += pow(Math.abs(a[i + aOffset] - b[i + bOffset]));
            if(dis > thresPow)
                return (i+1);
        }
        return -length;
    }

    public long addEarlySum = 0;
    public long addDisSum = 0;

    @Override
    public String toString() {
        return "LNorm_" + p;
    }
}

//package com.corp.tsfile.research.distance;
//
///**
// * Created by kangrong on 16/12/27.
// */
//public class LNormdouble implements Distance{
//    public double getP() {
//        return p;
//    }
//
//    private double p;
//
//    public LNormdouble(int p) {
//        assert p >= 1;
//        this.p = p;
//    }
//
//    @Override
//    public double distWithoutSqrt(double a, double b) {
//        return Math.pow(Math.abs(a-b),p);
//    }
//
////    @Override
////    public double distNoRoot(double up, double lowerLine) {
////        return (double) Math.pow(Math.abs(up - lowerLine), p);
////    }
//
//    @Override
//    public double getThresNoRoot(double thres) {
//        return Math.pow(thres, p);
//    }
//
//
//    public double dist(double[] a, double[] b){
//        assert a.length == b.length;
//        double dis = 0;
//        for (int i = 0; i < a.length; i++) {
//             dis += Math.pow(Math.abs(a[i]-b[i]),p);
//        }
//        return Math.pow(dis,1.0/p);
//    }
//
//    public double dist(double[] a, int aOffset, double[] b, int bOffset, int length){
//        assert a.length >= aOffset+length && a.length >= bOffset+length;
//        double dis = 0;
//        for (int i = 0; i < length; i++) {
////            addDisSum++;
//            dis += Math.pow(Math.abs(a[i+aOffset]-b[i+bOffset]),p);
//        }
//        return Math.pow(dis,1.0/p);
//    }
//
//    @Override
//    public int distEarlyAbandon(double[] a, int aOffset, double[] b, int bOffset, int length, double thres) {
//        assert a.length >= aOffset + length && b.length >= bOffset + length;
//        double thresPow = Math.pow(thres, p);
//        double dis = 0;
////        for (int i = length - 1; i >= 0; i--) {
////            dis += Math.pow(Math.abs(a[i + aOffset] - b[i + bOffset]), p);
////            if(dis > thresPow)
////                return i;
////        }
//        for (int i = 0; i < length; i++) {
//            dis += Math.pow(Math.abs(a[i + aOffset] - b[i + bOffset]), p);
////            addEarlySum++;
//            if(dis > thresPow)
//                return i;
//        }
//        return -1;
//    }
//    public long addEarlySum = 0;
//    public long addDisSum = 0;
//}
