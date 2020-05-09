package org.apache.iotdb.db.index.common;


import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

public class UnsupportedIndexQueryException extends QueryProcessException {

  private static final long serialVersionUID = 4185676677334759039L;

  public UnsupportedIndexQueryException(String message) {
    super(message, TSStatusCode.INDEX_PROCESS_ERROR.getStatusCode());
  }

  public UnsupportedIndexQueryException(String message, int errorCode) {
    super(message, errorCode);
  }

  public UnsupportedIndexQueryException(IoTDBException e) {
    super(e);
  }
}
