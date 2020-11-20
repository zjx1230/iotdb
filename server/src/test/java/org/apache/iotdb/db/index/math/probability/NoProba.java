package org.apache.iotdb.db.index.math.probability;

/**
 * Created by kangrong on 17/1/3.
 */
public class NoProba extends ProbUtil {
    @Override
    public double getRangeProbability(double up, double down) {
        return 1;
    }

    @Override
    public double getNextRandom() {
        throw new UnsupportedOperationException("NoProba cannot get next random value!");
    }
}
