package org.apache.iotdb.db.index.algorithm.elb.lowerbound;


import org.apache.iotdb.db.index.algorithm.elb.NTolsPattern;
import org.apache.iotdb.db.index.distance.Distance;

/**
 * 这个类用于论文代码的Pattern生成，由于阈值都已经指定，因此构造函数直接包含了分段信息，而真正的Pattern需要传入一个thres_factor因子来构建
 * 这个只适用于人工数据集，对于真实数据集，threshold已经定好，不改变，不需要这个类生成
 * Created by kangrong on 17/1/13.
 */
public class ELBPatternParamGenerator {
    private final double[] dataPoints;
    private final int segCount;
    public final Distance distance;
    public final int dataLength;
    public final int[] tolerIndexes;
    public double[] valueRange;
    public int[] subLens;


    public ELBPatternParamGenerator(double[] dataPoints, int[] indexArray, Distance distance) {
        this.dataPoints = dataPoints;
        dataLength = dataPoints.length;
        this.segCount = indexArray.length;
        this.distance = distance;
        valueRange = new double[segCount];
        subLens = new int[segCount];
        this.tolerIndexes = indexArray;

        for (int i = 0; i < tolerIndexes.length - 1; i++) {
            subLens[i] = tolerIndexes[i + 1] - tolerIndexes[i];
        }
        subLens[subLens.length - 1] = dataPoints.length - tolerIndexes[tolerIndexes.length - 1];
        for (int i = 0; i < segCount - 1; i++) {
            double max = -Double.MAX_VALUE;
            double min = Double.MAX_VALUE;
            for (int j = tolerIndexes[i]; j < tolerIndexes[i + 1]; j++) {
                if (dataPoints[j] < min)
                    min = dataPoints[j];
                if (dataPoints[j] > max) {
                    max = dataPoints[j];
                }
            }
            valueRange[i] = max - min;
        }
        double max = -Double.MAX_VALUE;
        double min = Double.MAX_VALUE;
        for (int j = tolerIndexes[segCount - 1]; j < dataPoints.length; j++) {
            if (dataPoints[j] < min)
                min = dataPoints[j];
            if (dataPoints[j] > max) {
                max = dataPoints[j];
            }
        }
        valueRange[segCount - 1] = max - min;
    }

    public NTolsPattern getDiffBoundRadius(double thres, int boundRadius) {
        double[] newToleranceArray = new double[segCount];
        for (int i = 0; i < segCount; i++) {
            newToleranceArray[i] = valueRange[i] * thres;
        }
        return new NTolsPattern(dataPoints, tolerIndexes, newToleranceArray, boundRadius, distance);
    }

}
