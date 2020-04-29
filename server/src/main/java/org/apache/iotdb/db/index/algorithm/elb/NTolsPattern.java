package org.apache.iotdb.db.index.algorithm.elb;

import java.util.Arrays;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LNormdouble;

/**
 * Created by dytwest on 2017/5/15.
 */
public class NTolsPattern{
    //pattern的数据点
    /**
     * 表示方法：对于pattern dataPoints：[1,2,3,4,5,6,7,8]
     * <pre>
     * startTolIndex:[0,2,7]
     * endTolIndex:[0,5,8]
     * toleranceArray:[0.1,0.4,0.7]
     * 则有：
     * 第一个分割点在Index[1]，第二段允许从4或者5开始，第三段允许从7或8开始。
     * [1,2,3,4,5,6,7,8]
     * </pre>
     */
    public int[] tolerIndexes;
    public double[] dataPoints;
    public final int[] startTolIndex;
    public final int[] endTolIndex;
    public double[] diff;
    public double[] tolsPower;
    public int[] idx_set;
    public final Distance distance;
    public int length;
    public int segmengLength;
    public int[] subLens;
    public double[] toleranceArray;

    public NTolsPattern(double[] dataPoints, int[] tolerIndexes, double[] toleranceArray, int groupLen, Distance distance) {
        this.dataPoints = dataPoints;
        this.length = dataPoints.length;
        segmengLength = toleranceArray.length;
        assert tolerIndexes.length == toleranceArray.length && tolerIndexes[0] == 0;
        this.tolerIndexes = tolerIndexes;
        this.toleranceArray = toleranceArray;
//        subLens = new int[toleranceArray.length];
//        for (int i = 0; i < tolerIndexes.length - 1; i++) {
//            subLens[i] = tolerIndexes[i + 1] - tolerIndexes[i];
//        }
//        subLens[segmengLength - 1] = dataPoints.length - tolerIndexes[segmengLength - 1];

        this.distance = distance;
        this.startTolIndex = new int[segmengLength + 1];
        this.endTolIndex = new int[segmengLength + 1];
        this.startTolIndex[0] = 0;
        this.endTolIndex[0] = 0;
        for (int i = 1; i < segmengLength; i++) {
            this.startTolIndex[i] = tolerIndexes[i] - groupLen;
            this.endTolIndex[i] = tolerIndexes[i] + groupLen;
        }
        this.startTolIndex[segmengLength] = length;
        this.endTolIndex[segmengLength] = length;
        checkRadius();
        this.subLens = new int[toleranceArray.length];
        //最大变动长度，代码略繁，为了以后任意长度区间都可以用
        int maxRadius = -1;
        for (int i = 0; i < segmengLength; i++) {
            if (endTolIndex[i] - startTolIndex[i] > maxRadius)
                maxRadius = endTolIndex[i] - startTolIndex[i];
        }
        diff = new double[maxRadius + 1];
        idx_set = new int[maxRadius + 1];
        tolsPower = new double[toleranceArray.length];
        for (int i = 0; i < toleranceArray.length; i++) {
            if (distance instanceof LNormdouble)
                tolsPower[i] = ((LNormdouble) distance).pow(toleranceArray[i]);
//            else
//                throw new UnsupportedOperationException();
        }
    }

    private void checkRadius() {
        for (int i = 0; i < segmengLength; i++) {
            if (!(startTolIndex[i] <= endTolIndex[i]) || !(endTolIndex[i] < startTolIndex[i + 1])) {
                System.out.println("Error Radius");
                System.out.println(Arrays.toString(startTolIndex));
                System.out.println(Arrays.toString(endTolIndex));
                System.exit(0);
            }
        }
    }

    public int distEarlyAbandon(Distance disMetric, double[] stream, int offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * add by dyt : Exact-Calculating Algorithm
     * 返回负数为正确；返回正数是错误
     *
     * @param disMetric
     * @param stream
     * @param offset
     * @return
     */
    public int distEarlyAbandonDetail(Distance disMetric, double[] stream, int offset) {
        LNormdouble normdouble;
        if (disMetric instanceof LNormdouble)
            normdouble = (LNormdouble) disMetric;
        else
            throw new UnsupportedOperationException();
        double left_sum = 0;
//        double p = disMetric.getP();
        int i = 1;
        for (; i < this.toleranceArray.length; i++) {
            boolean set_flag = false;
            int j;
            for (j = this.endTolIndex[i - 1]; j < this.startTolIndex[i] - 1; j++)
//                left_sum += Math.pow(Math.abs(dataPoints[j] - stream[j + offset]), p)
//                        - Math.pow(this.toleranceArray[i-1], p);
                left_sum += normdouble.pow(Math.abs(dataPoints[j] - stream[j + offset]))
                        - normdouble.pow(this.toleranceArray[i - 1]);
            int k = 0;
//            int j = startTolIndex[i] - 1;
            for (; j < this.endTolIndex[i]; j++, k++) {
//                diff = new double[this.endTolIndex[i-1] - this.startTolIndex[i]];
//                idx_set = new int[this.endTolIndex[i-1] - this.startTolIndex[i]];
                diff[k] = normdouble.pow(Math.abs(dataPoints[j] - stream[j + offset]));
                left_sum += diff[k] - tolsPower[i - 1];
                if (left_sum <= 0) {
                    idx_set[k] = j;
                    set_flag = true;
                } else
                    idx_set[k] = -1;
            }
            if (!set_flag)
                return 1;

            double next_sum = 0;
            //当分割点为e_i的特殊情况，此时对应前一段到e_i-1的情况，前一段可行则min_sum为0否则设为最大
            double min_sum = (idx_set[--k] > 0) ? 0 : Double.MAX_VALUE;
//            k--;
            for (; k > 0; ) {
                next_sum += diff[k] - tolsPower[i];
                if (idx_set[--k] > 0 && next_sum < min_sum)
                    min_sum = next_sum;
            }
            left_sum = min_sum;
        }
        for (int j = this.endTolIndex[i - 1]; j < this.length; j++)
            left_sum += normdouble.pow(Math.abs(dataPoints[j] - stream[j + offset]))
                    - normdouble.pow(this.toleranceArray[i - 1]);
        return (left_sum <= 0) ? -1 : 1;
    }

    public boolean dist(Distance disMetric, double[] stream, int offset) {
        throw new UnsupportedOperationException();
//        double dis;
//        int i;
//        for (i = 0; i < tolerIndexes.length; i++) {
//            dis = disMetric.dist(stream, offset + tolerIndexes[i], dataPoints, tolerIndexes[i], subLens[i]);
//            if (dis > toleranceArray[i])
//                return false;
//        }
//        return true;
    }

    public int distEarlyAbandonGrain(Distance disMetric, double[] stream, int offset, int[] bps) {
        int ret = 0;
        int len;
        for (int i = 0; i < segmengLength; i++) {
            len = bps[i + 1] - bps[i];
            int count = disMetric.distEarlyAbandonDetailNoRoot(stream, offset + bps[i],
                    dataPoints, bps[i], len, tolsPower[i]*len);
            if (count > 0)
                return ret + count;
            else
                ret -= count;
        }
        return -ret;
    }

    public int distDetailGrain(Distance disMetric, double[] stream, int offset, int[] bps) {
        int ret = 0;
        int len;
        double dis;
        int i;
        for (i = 0; i < tolerIndexes.length; i++) {
            len = bps[i + 1] - bps[i];
            dis = disMetric.distPower(stream, offset + bps[i], dataPoints, bps[i], len);
            ret += len;
            if (dis > tolsPower[i]*len)
                return ret;
        }
        return -ret;
    }

}
