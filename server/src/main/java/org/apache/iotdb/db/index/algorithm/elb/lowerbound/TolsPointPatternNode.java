package org.apache.iotdb.db.index.algorithm.elb.lowerbound;

import org.apache.iotdb.db.index.algorithm.elb.NTolsPattern;

/**
 * PatternNode：Pattern被分割成的小block，lowerbound 1
 * Created by kangrong on 16/12/27.
 */
public class TolsPointPatternNode extends PatternNode {

    /**
     * @param data
     * @param offset - i*WindowBlockSize
     * @param len
     * @param thres
     */
    public TolsPointPatternNode(double[] data, int offset, int len, double thres) {
        actualLower = Double.MAX_VALUE;
        actualUpper = -Double.MAX_VALUE;
        int end = offset + len;
//        if(offset == 230)
//            System.out.println();
        for (int i = offset; i < end && i < data.length; i++) {
            double f = data[i];
            if (f < actualLower)
                actualLower = f;
            if (f > actualUpper)
                actualUpper = f;
        }
        lowerLine = actualLower - thres;
        upperLine = actualUpper + thres;
//        this.windowUnitSize = data.length;
    }

    private TolsPointPatternNode() {
        actualLower = Double.MAX_VALUE;
        actualUpper = -Double.MAX_VALUE;
        lowerLine = Double.MAX_VALUE;
        upperLine = -Double.MAX_VALUE;
    }

    /**
     * 构造多阈值的patternNode数组
     *
     * @param isOutput
     * @param pattern
     * @param windowBlockSize
     * @return
     */
    public static TolsPointPatternNode[] getNodesFromMultiTolerance(boolean isOutput, NTolsPattern pattern,
                                                                    int windowBlockSize, PatternEnvelope envelope) {
        int nodeLength = (int) Math.floor(((double) pattern.dataPoints.length) / windowBlockSize);
        TolsPointPatternNode[] pointPttNodes = new TolsPointPatternNode[nodeLength];
//            System.out.println("pattern:" + i);
//            System.out.println();
//        double[][] lowerbound = new double[nodeLength][];
        for (int i = 0; i < nodeLength; i++) {
            //get the largest tolerances in this pttNode windows
            pointPttNodes[i] = new TolsPointPatternNode();
            for (int j = i * windowBlockSize; j < (i + 1) * windowBlockSize; j++) {
                if (envelope.upperLine[j] > pointPttNodes[i].upperLine)
                    pointPttNodes[i].upperLine = envelope.upperLine[j];
                if (envelope.lowerLine[j] < pointPttNodes[i].lowerLine)
                    pointPttNodes[i].lowerLine = envelope.lowerLine[j];
                if (envelope.valueLine[j] > pointPttNodes[i].actualUpper)
                    pointPttNodes[i].actualUpper = envelope.valueLine[j];
                if (envelope.valueLine[j] < pointPttNodes[i].actualLower)
                    pointPttNodes[i].actualLower = envelope.valueLine[j];
            }
//            if (isOutput) {
//                lowerbound[i] = new double[]{pointPttNodes[i].upperLine,
//                        pointPttNodes[i].lowerLine,
//                        pointPttNodes[i].actualUpper,
//                        pointPttNodes[i].actualLower};
//            }
        }
        return pointPttNodes;
    }

}
