package org.apache.iotdb.db.index.algorithm.elb.lowerbound;

import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * 即Pattern Block，PatternNode：Pattern被分割成的小block。
 * 其中actual upper
 * Created by kangrong on 16/12/27.
 */
public abstract class PatternNode {
    public double actualUpper;
    public double actualLower;
    //真正有用的上界
    public double upperLine;
    //真正有用的下界
    public double lowerLine;

    public boolean contains(double value) {
        return upperLine >= value && lowerLine <= value;
    }

    @Override
    public String toString() {
        return String.format("upperLine: %.2f, lowerLine:%.2f, actualUpper:%.2f, actualLower:%.2f",
            upperLine, lowerLine, actualUpper, actualLower);
    }
}
