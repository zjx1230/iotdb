package org.apache.iotdb.db.index.algorithm.elb.pattern;

import org.apache.iotdb.db.utils.datastructure.TVList;

public interface CalcParam {

  /**
   * <p>Given a subsequence (specified by {@code tvList},{@code offset},{@code length}, calculate
   * the pattern parameters: subpattern thresholds, subpattern border ranges.</p>
   *
   * <p>The {@code borderRanges} will support variable border mode.</p>
   *
   * @return [subpatternCount, thresholdsArray, minLeftBorders, maxLeftBorders]
   */
  Object[] refreshParam(TVList tvList, int offset, int length);

}
