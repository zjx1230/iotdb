// package org.apache.iotdb.db.utils.datastructure;
//
// import org.apache.iotdb.tsfile.exception.NotImplementedException;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
// import org.apache.iotdb.tsfile.read.TimeValuePair;
//
// public class TruncatedTVListWrapper extends TVList {
//
//  private final TVList srcData;
//  private static final String UNMODIFIED_ERROR_MSG = "TVListUnmodifiedWrapper is read-only";
//  private int alignedLength;
//
//  public TruncatedTVListWrapper(TVList srcData, int alignedLength) {
//    this.srcData = srcData;
//    this.alignedLength = alignedLength;
//  }
//
//  @Override
//  public void sort() {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void set(int src, int dest) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void setFromSorted(int src, int dest) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void setToSorted(int src, int dest) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void reverseRange(int lo, int hi) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void expandValues() {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  public TVList clone() {
//    return new TruncatedTVListWrapper(srcData, this.alignedLength);
//  }
//
//  @Override
//  protected void releaseLastValueArray() {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void clearValue() {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void clearSortedValue() {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void saveAsPivot(int pos) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected void setPivotTo(int pos) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  public TimeValuePair getTimeValuePair(int index) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  protected TimeValuePair getTimeValuePair(int index, long time, Integer floatPrecision,
//      TSEncoding encoding) {
//    throw new UnsupportedOperationException(UNMODIFIED_ERROR_MSG);
//  }
//
//  @Override
//  public TSDataType getDataType() {
//    return srcData.getDataType();
//  }
//
//  @Override
//  public int size() {
//    return alignedLength;
//  }
//
//  private int transformIdx(int idx) {
//    return idx < alignedLength && idx > srcData.size() ? srcData.size() - 1 : idx;
//  }
//
//  @Override
//  public double getDouble(int index) {
//    return srcData.getDouble(transformIdx(index));
//  }
//
//  @Override
//  public int getInt(int index) {
//    return srcData.getInt(transformIdx(index));
//  }
//
//  @Override
//  public float getFloat(int index) {
//    return srcData.getFloat(transformIdx(index));
//  }
//
//  @Override
//  public long getLong(int index) {
//    return srcData.getLong(transformIdx(index));
//  }
//
//  @Override
//  public String toString() {
//    StringBuilder sb = new StringBuilder();
//    sb.append("{");
//    for (int i = 0; i < alignedLength; i++) {
//      TimeValuePair pair = srcData.getTimeValuePair(i);
//      switch (getDataType()) {
//        case INT32:
//          sb.append(String.format("[%d,%d],", pair.getTimestamp(), pair.getValue().getInt()));
//          break;
//        case INT64:
//          sb.append(String.format("[%d,%d],", pair.getTimestamp(), pair.getValue().getLong()));
//          break;
//        case FLOAT:
//          sb.append(String.format("[%d,%.2f],", pair.getTimestamp(), pair.getValue().getFloat()));
//          break;
//        case DOUBLE:
//          sb.append(String.format("[%d,%.2f],", pair.getTimestamp(),
// pair.getValue().getDouble()));
//          break;
//        default:
//          throw new NotImplementedException(getDataType().toString());
//      }
//    }
//    sb.append("}");
//    return sb.toString();
//  }
// }
