package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.utils.datastructure.TVList;

public class TVListPointer {
  public TVList tvList;
  public int offset;
  public int length;

  public TVListPointer(TVList tvList, int offset, int length) {
    this.tvList = tvList;
    this.offset = offset;
    this.length = length;
  }
}
