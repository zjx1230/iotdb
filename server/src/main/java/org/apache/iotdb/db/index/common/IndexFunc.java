package org.apache.iotdb.db.index.common;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public enum IndexFunc {
  SIM_ST(INT64), SIM_ET(INT64), DIST(DOUBLE), LEN(INT32), CATEGORY, UNKNOWN;

  private TSDataType type;

  IndexFunc(TSDataType type) {
    this.type = type;
  }

  /**
   * For the index function with arbitrary or uncertain data type，it can always be converted into a
   * string by {@code Object.toString()}.
   */
  IndexFunc() {
    this.type = TSDataType.TEXT;
  }


  public static List<TSDataType> getSeriesByFunc(List<Path> paths, List<String> aggregations) {
    List<TSDataType> tsDataTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = getIndexFunc(aggregations.get(i)).getType();
      tsDataTypes.add(dataType);
    }
    return tsDataTypes;
  }

  public TSDataType getType() {
    return type;
  }

  public static IndexFunc getIndexFunc(String indexFuncString) {
    String normalized = indexFuncString.toUpperCase();
    switch (normalized) {
      case "SIM_ST":
        return SIM_ST;
      case "SIM_ET":
        return SIM_ET;
      case "DIST":
        return DIST;
      case "LEN":
        return LEN;
      default:
//        return UNKNOWN;
        throw new IllegalIndexParamException("never seen index func:" + indexFuncString);
    }
  }
}
