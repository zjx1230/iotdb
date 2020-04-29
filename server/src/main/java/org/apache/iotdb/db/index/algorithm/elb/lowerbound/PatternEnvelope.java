package org.apache.iotdb.db.index.algorithm.elb.lowerbound;


import org.apache.iotdb.db.index.algorithm.elb.NTolsPattern;

/**
 * 计算包络线，其实只对point有用。
 * Created by kangrong on 17/1/9.
 */
public class PatternEnvelope {
    public double[] upperLine;
    public double[] valueLine;
    public double[] lowerLine;

    public PatternEnvelope(NTolsPattern pattern) {
        upperLine = new double[pattern.length];
        valueLine = new double[pattern.length];
        lowerLine = new double[pattern.length];
        initNTolsPattern(pattern);
    }

    private void initTolsPattern(NTolsPattern pattern) {
        int i;
        for (i = 0; i < pattern.tolerIndexes.length - 1; i++) {
            for (int j = pattern.tolerIndexes[i]; j < pattern.tolerIndexes[i + 1]; j++) {
                upperLine[j] = pattern.dataPoints[j] + pattern.toleranceArray[i];
                valueLine[j] = pattern.dataPoints[j];
                lowerLine[j] = pattern.dataPoints[j] - pattern.toleranceArray[i];
            }
        }
        for (int j = pattern.tolerIndexes[i]; j < pattern.length; j++) {
            upperLine[j] = pattern.dataPoints[j] + pattern.toleranceArray[i];
            valueLine[j] = pattern.dataPoints[j];
            lowerLine[j] = pattern.dataPoints[j] - pattern.toleranceArray[i];
        }
    }

    private double mtt(NTolsPattern pattern, int k) {
//        int end = (k + 1 == pattern.segmengLength) ? pattern.length : pattern.endTolIndex[k + 1];
        return Math.pow(pattern.endTolIndex[k + 1] - pattern.startTolIndex[k], 1 / pattern.distance.getP())
                * pattern.toleranceArray[k];
    }

    private void initNTolsPattern(NTolsPattern pattern) {
//        System.out.println(Arrays.toString(pattern.startTolIndex));
//        System.out.println(Arrays.toString(pattern.endTolIndex));
        int i;
        double tol;
        //i即公式中的k
        for (i = 0; i < pattern.toleranceArray.length; i++) {
//            变动区，[s_k,e_k)
            for (int j = pattern.startTolIndex[i]; j < pattern.endTolIndex[i]; j++) {
                double left = mtt(pattern, i - 1);
                double right = mtt(pattern, i);
                tol = left > right ? left : right;
                upperLine[j] = pattern.dataPoints[j] + tol;
                valueLine[j] = pattern.dataPoints[j];
                lowerLine[j] = pattern.dataPoints[j] - tol;
            }

//            System.out.println("testest");
//            非变动区，[e_k,s_k+1)

            int m = (i + 1 == pattern.segmengLength) ? pattern.length : pattern.startTolIndex[i + 1];
            for (int j = pattern.endTolIndex[i]; j < m; j++) {
                tol = mtt(pattern, i);
                upperLine[j] = pattern.dataPoints[j] + tol;
                valueLine[j] = pattern.dataPoints[j];
                lowerLine[j] = pattern.dataPoints[j] - tol;
            }
//            System.out.println("testest");
        }
    }
}
