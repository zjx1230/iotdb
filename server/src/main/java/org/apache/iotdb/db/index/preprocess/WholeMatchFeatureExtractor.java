package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.db.index.common.IndexConstant.NON_IMPLEMENTED_MSG;

import java.nio.ByteBuffer;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public abstract class WholeMatchFeatureExtractor extends IndexFeatureExtractor {

  protected TVList srcData;
  protected boolean hasNewData;

  public WholeMatchFeatureExtractor(TSDataType dataType,
      boolean inQueryMode) {
    super(dataType, inQueryMode);
  }

  public void appendNewSrcData(TVList newData) {
    this.srcData = newData;
    hasNewData = true;
  }

  public void appendNewSrcData(BatchData newData) {
    throw new UnsupportedOperationException(NON_IMPLEMENTED_MSG);
  }


  @Override
  public boolean hasNext() {
    return hasNewData;
  }

  @Override
  public ByteBuffer closeAndRelease() {
    // Not data to be stored
    return ByteBuffer.allocate(0);
  }
}
