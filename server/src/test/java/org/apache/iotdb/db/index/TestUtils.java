package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXING_SUFFIX;

import java.io.IOException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

public class TestUtils {

  public static String TEST_INDEX_FILE_NAME = "test_index";

  public static void clearIndexFile(String index_name) {
    FSFactoryProducer.getFSFactory().getFile(index_name).delete();
    FSFactoryProducer.getFSFactory().getFile(index_name + INDEXED_SUFFIX).delete();
    FSFactoryProducer.getFSFactory().getFile(index_name + INDEXING_SUFFIX).delete();
  }

  public static String tvListToString(TVList tvList) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < tvList.size(); i++) {
      TimeValuePair pair = tvList.getTimeValuePair(i);
      switch (tvList.getDataType()) {
        case INT32:
          sb.append(String.format("[%d,%d],", pair.getTimestamp(), pair.getValue().getInt()));
          break;
        case INT64:
          sb.append(String.format("[%d,%d],", pair.getTimestamp(), pair.getValue().getLong()));
          break;
        case FLOAT:
          sb.append(String.format("[%d,%.2f],", pair.getTimestamp(), pair.getValue().getFloat()));
          break;
        case DOUBLE:
          sb.append(String.format("[%d,%.2f],", pair.getTimestamp(), pair.getValue().getDouble()));
          break;
        default:
          throw new NotImplementedException(tvList.getDataType().toString());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  private static String doubleToFormatStringCoverInf(double d) {
    if (d == Double.MAX_VALUE) {
      return "Inf";
    } else if (d == -Double.MAX_VALUE) {
      return "-Inf";
    } else {
      return String.format("%.2f", d);
    }
  }

  public static String TwoDimDoubleArrayToString(double[][] mbrs) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (double[] mbr : mbrs) {
      sb.append(String.format("[%s,%s],",
          doubleToFormatStringCoverInf(mbr[0]),
          doubleToFormatStringCoverInf(mbr[1])));
    }
    sb.append("}");
    return sb.toString();
  }


}
