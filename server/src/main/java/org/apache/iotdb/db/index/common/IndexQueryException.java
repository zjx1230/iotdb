package org.apache.iotdb.db.index.common;


import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

public class IndexQueryException extends QueryProcessException {


  private static final long serialVersionUID = 9019233783504576296L;

  public IndexQueryException(String message) {
    super(message, TSStatusCode.INDEX_PROCESS_ERROR.getStatusCode());
  }

  public IndexQueryException(String message, int errorCode) {
    super(message, errorCode);
  }

  public IndexQueryException(IoTDBException e) {
    super(e);
  }
}
