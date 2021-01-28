package org.apache.iotdb.db.index.common.math.probability;

import java.util.Random;

/**
 * Created by kangrong on 17/1/2.
 */
public class UniformProba extends ProbUtil {
    private final double upBound;
    private final double downBound;
    private final double range;
    private Random r;

    public UniformProba(double upBound, double downBound) {
        assert upBound > downBound;
        this.upBound = upBound;
        this.downBound = downBound;
        this.range = upBound - downBound;
        this.r = new Random();
    }
    public UniformProba(double upBound, double downBound, Random r) {
        assert upBound > downBound;
        this.upBound = upBound;
        this.downBound = downBound;
        this.range = upBound - downBound;
        this.r = r;
    }

    @Override
    public double getRangeProbability(double up, double down) {
        assert up > down;
        if (up < downBound || down > upBound)
            return 0;
        return (Math.min(up, upBound) - Math.max(down, downBound)) / range;
    }

    public static double getRangeProbability(double upBound, double downBound, double up, double down) {
        assert up > down && upBound > downBound;
        if (up < downBound || down > upBound)
            return 0;
        return (Math.min(up, upBound) - Math.max(down, downBound)) / (upBound-downBound);
    }

    @Override
    public double getNextRandom() {
        return r.nextDouble() * range + downBound;
    }

    private static double normsDist(double a) {
        double p = 0.2316419;
        double b1 = 0.31938153;
        double b2 = -0.356563782;
        double b3 = 1.781477937;
        double b4 = -1.821255978;
        double b5 = 1.330274429;

        double x = Math.abs(a);
        double t = 1 / (1 + p * x);

        double val = 1 - (1 / (Math.sqrt(2 * Math.PI)) * Math.exp(-1 * Math.pow(a, 2) / 2)) * (b1 * t + b2 * Math.pow(t, 2) + b3 * Math.pow(t, 3) + b4 * Math.pow(t, 4) + b5 * Math.pow(t, 5));

        if (a < 0) {
            val = 1 - val;
        }

        return val;
    }


}
