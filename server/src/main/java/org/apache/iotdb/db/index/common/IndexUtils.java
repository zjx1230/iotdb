package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class IndexUtils {

  public static int getDataTypeSize(TVList srcData) {
    return getDataTypeSize(srcData.getDataType());
  }

  public static int getDataTypeSize(PrimitiveList srcData) {
    return getDataTypeSize(srcData.getTsDataType());
  }

  public static int getDataTypeSize(TSDataType dataType){
    switch (dataType) {
      case INT32:
      case FLOAT:
        return 4;
      case INT64:
      case DOUBLE:
        return 8;
      default:
        throw new NotImplementedException(dataType.toString());
    }
  }

  public static double getDoubleFromAnyType(TVList srcData, int idx) {
    switch (srcData.getDataType()) {
      case INT32:
        return srcData.getInt(idx);
      case INT64:
        return srcData.getLong(idx);
      case FLOAT:
        return srcData.getFloat(idx);
      case DOUBLE:
        return srcData.getDouble(idx);
      default:
        throw new NotImplementedException(srcData.getDataType().toString());
    }
  }

  public static double getValueRange(TVList srcData, int offset, int length) {
    double minValue = Double.MAX_VALUE;
    double maxValue = Double.MIN_VALUE;
    for (int idx = offset; idx < offset + length; idx++) {
      if (idx >= srcData.size()) {
        break;
      }
      double v = getDoubleFromAnyType(srcData, idx);
      if (v < minValue) {
        minValue = v;
      }
      if (v > maxValue) {
        maxValue = v;
      }
    }
    return maxValue - minValue;
  }
}
