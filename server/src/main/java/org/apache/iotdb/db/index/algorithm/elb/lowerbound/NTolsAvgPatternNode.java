package org.apache.iotdb.db.index.algorithm.elb.lowerbound;


import org.apache.iotdb.db.index.algorithm.elb.NTolsPattern;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.index.distance.LNormdouble;

/**
 * Created by dytwest on 2017/5/15.
 */
public class NTolsAvgPatternNode extends PatternNode {

    public double[] sumValues;
    public double[] sumUppers;
    public double[] sumLowers;

    protected NTolsAvgPatternNode() {
        actualLower = Double.MAX_VALUE;
        actualUpper = -Double.MAX_VALUE;
        lowerLine = Double.MAX_VALUE;
        upperLine = -Double.MAX_VALUE;
    }

    public NTolsAvgPatternNode(double[] data, int offset, int len, double thres, int normInt) {
        assert offset >= len;
        lowerLine = Double.MAX_VALUE;
        upperLine = -Double.MAX_VALUE;
        for (int i = 0; i < len && offset + i < data.length; i++) {
            double sum = 0;
            for (int j = 0; j < len; j++) {
                try {
                    sum += data[offset + i - j];
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            double avg = sum / len;
            if (avg < lowerLine)
                lowerLine = avg;
            if (avg > upperLine)
                upperLine = avg;
        }
        switch (normInt) {
            case 1:
                lowerLine -= thres / len;
                upperLine += thres / len;
                break;
            case 2:
                lowerLine -= thres / Math.sqrt(len);
                upperLine += thres / Math.sqrt(len);
                break;
            default:
                assert false;
        }
    }


    /**
     * 构造多阈值的patternNode数组
     *
     * @param isOutput
     * @param pattern
     * @param windowBlockSize
     * @return
     */
    public static NTolsAvgPatternNode[] getNodesFromMultiTolerance(boolean isOutput, NTolsPattern pattern,
                                                                  int windowBlockSize, Distance distance) {
        int nodeLength = (int) Math.floor(((double) pattern.dataPoints.length) / windowBlockSize);
        NTolsAvgPatternNode[] avgPttNodes = new NTolsAvgPatternNode[nodeLength];
        if (isOutput)
            System.out.println("pattern:");
        for (int i = 1; i < nodeLength; i++) {
            //get the largest tolerances in this pttNode windows
            avgPttNodes[i] = new NTolsAvgPatternNode();
            avgPttNodes[i].sumLowers = new double[windowBlockSize];
            avgPttNodes[i].sumValues = new double[windowBlockSize];
            avgPttNodes[i].sumUppers = new double[windowBlockSize];

            //j是对齐到各种偏移时的起点，最左端从上一个段+1开始，最右是正好对齐，即i * windowBlockSize
            for (int j = (i - 1) * windowBlockSize + 1; j <= i * windowBlockSize; j++) {
                int end = j + windowBlockSize - 1;
                int startIndex = j - ((i - 1) * windowBlockSize + 1);
                //get sum of ε^p
                double interval = getEpsilonSum(pattern, distance,j, end, windowBlockSize);

                for (int l = j; l <= end; l++)
                    avgPttNodes[i].sumValues[startIndex] += pattern.dataPoints[l];
                avgPttNodes[i].sumLowers[startIndex] = avgPttNodes[i].sumValues[startIndex] - interval;
                avgPttNodes[i].sumUppers[startIndex] = avgPttNodes[i].sumValues[startIndex] + interval;
                //更新
                if (avgPttNodes[i].sumUppers[startIndex] > avgPttNodes[i].upperLine)
                    avgPttNodes[i].upperLine = avgPttNodes[i].sumUppers[startIndex];
                if (avgPttNodes[i].sumLowers[startIndex] < avgPttNodes[i].lowerLine)
                    avgPttNodes[i].lowerLine = avgPttNodes[i].sumLowers[startIndex];
                if (avgPttNodes[i].sumValues[startIndex] > avgPttNodes[i].actualUpper)
                    avgPttNodes[i].actualUpper = avgPttNodes[i].sumValues[startIndex];
                if (avgPttNodes[i].sumValues[startIndex] < avgPttNodes[i].actualLower)
                    avgPttNodes[i].actualLower = avgPttNodes[i].sumValues[startIndex];
            }
        }
//        if (isOutput)
//            CSVExporter.exportMultiColumn(lowerbound, "point_pattern");
        return avgPttNodes;
    }

    /**
     * Calculate ε_sum(i)算法。end即为i，start即为ws。
     * @param pattern
     * @param distance
     * @param start 即ws
     * @param end
     * @param windowBlockSize
     * @return
     */
    private static double getEpsilonSum(NTolsPattern pattern, Distance distance, int start, int end, int windowBlockSize) {
        double p = distance.getP();
        double interval = 0;
        //Line2~Line3，求m和n
        int m, n;
        for (m = 0; m < pattern.segmengLength; m++)
            if (start < pattern.endTolIndex[m])
                break;
        for (n = pattern.segmengLength - 1; n < 0; n--)
            if (end >= pattern.startTolIndex[n])
                break;
        if(m == 0)
            System.out.println();
        if (distance instanceof LNormdouble) {
            //Line 4，左边
            double sum = (pattern.endTolIndex[m - 1] - pattern.startTolIndex[m - 1]) *
                    distance.getThresNoRoot(pattern.toleranceArray[m - 1]);
            //Line 5~7，中间
            for (int i = pattern.endTolIndex[m - 1]; i < pattern.startTolIndex[n + 1] - 1; i++) {
                sum += distance.getThresNoRoot(getAldMax(pattern, i));
            }
            //Line 8，右边
            sum += (pattern.endTolIndex[n + 1] - pattern.startTolIndex[n + 1]) * distance.getThresNoRoot(pattern.toleranceArray[n]);
            interval =  Math.pow(sum / windowBlockSize, 1 / p) * windowBlockSize;
        } else if (distance instanceof LInfinityNormdouble) {
            double maxThres = -Double.MAX_VALUE;
            for (int i = m - 1; i < n + 1; i++) {
                double temp = distance.getThresNoRoot(pattern.toleranceArray[i]);
                if (temp > maxThres) maxThres = temp;
            }
            interval =  maxThres * windowBlockSize;
        }
        return interval;
    }

    private static double getAldMax(NTolsPattern pattern, int i){
        int k;
        for (k = 0; k < pattern.segmengLength; k++) {
            if(pattern.startTolIndex[k] <= i && i < pattern.startTolIndex[k+1])
                break;
        }
        if(i < pattern.endTolIndex[k])
            //公式6，Line1
            return Math.max(pattern.toleranceArray[k - 1], pattern.toleranceArray[k]);
        else
            //公式6，Line2
            return pattern.toleranceArray[k];
    }
}
