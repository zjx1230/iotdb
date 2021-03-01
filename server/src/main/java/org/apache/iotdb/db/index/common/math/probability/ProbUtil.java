package org.apache.iotdb.db.index.common.math.probability;

/** Created by kangrong on 17/1/2. */
public abstract class ProbUtil {

  public abstract double getRangeProbability(double up, double down);

  public abstract double getNextRandom();

  public static ProbUtil getNormal(ProbabilityType type, double max, double min) {
    switch (type) {
      case GAUSSION:
        return new GaussProba((max - min) / 6, (max + min) / 2);
      case UNIFORM:
        return new UniformProba(max, min);
      default:
        return new NoProba();
    }
  }

  public static double getPattern(
      ProbabilityType type, double pUp, double pDown, double max, double min) {
    switch (type) {
      case GAUSSION:
        return GaussProba.getRangeProbability((pUp - pDown) / 6, (pUp + pDown) / 2, max, min);
      case UNIFORM:
        return UniformProba.getRangeProbability(pUp, pDown, max, min);
      default:
        return 1;
    }
  }
}
