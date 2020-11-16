package org.apache.iotdb.db.index.router;

import static org.junit.Assert.*;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.router.IIndexRouter.Factory;
import org.apache.iotdb.db.metadata.PartialPath;
import org.junit.Assert;
import org.junit.Test;

public class ProtoIndexRouterTest {
//  private IndexInfo info1 = new IndexInfo();
  @Test
  public void serialize() throws IllegalPathException {
    IIndexRouter router = new ProtoIndexRouter("test_protoIndexRouter");
//    router.addIndexIntoRouter(new PartialPath("root.v.d1.s1"), );
  }


}