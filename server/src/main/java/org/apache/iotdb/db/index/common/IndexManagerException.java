package org.apache.iotdb.db.index.common;


import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

public class IndexManagerException extends QueryProcessException {

  private static final long serialVersionUID = 6261687971768311032L;

  public IndexManagerException(String message) {
    super(message, TSStatusCode.INDEX_PROCESS_ERROR.getStatusCode());
  }

  public IndexManagerException(String message, int errorCode) {
    super(message, errorCode);
  }

  public IndexManagerException(IoTDBException e) {
    super(e);
  }
}
