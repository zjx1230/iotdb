package org.apache.iotdb.db.index.usable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.junit.Assert;
import org.junit.Test;

public class MultiShortIndexUsabilityTest {

  @Test
  public void testMinusUsableRange() throws IllegalPathException, IOException {
    MultiShortIndexUsability usability = new MultiShortIndexUsability(null);
    // do nothing for addUsableRange
    usability.addUsableRange(new PartialPath("root.sg.d.s10"), 1, 2);
    usability.addUsableRange(new PartialPath("root.sg.d.s11"), 1, 2);

    usability.minusUsableRange(new PartialPath("root.sg.d.s1"), 1, 2);
    usability.minusUsableRange(new PartialPath("root.sg.d.s2"), 1, 2);
    usability.minusUsableRange(new PartialPath("root.sg.d.s3"), 1, 2);
    Set<PartialPath> ret = usability.getAllUnusableSeriesForWholeMatching();
    Assert.assertEquals("[root.sg.d.s3, root.sg.d.s2, root.sg.d.s1]", ret.toString());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    MultiShortIndexUsability usable2 = new MultiShortIndexUsability(null);
    usable2.deserialize(in);
    Set<PartialPath> ret2 = usability.getAllUnusableSeriesForWholeMatching();
    Assert.assertEquals("[root.sg.d.s3, root.sg.d.s2, root.sg.d.s1]", ret2.toString());
  }
}