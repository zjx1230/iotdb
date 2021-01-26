package org.apache.iotdb.db.index.usable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.junit.Assert;
import org.junit.Test;

public class SingleLongIndexUsabilityTest {

  @Test
  public void addUsableRange1() throws IOException, IllegalPathException {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
    // split range
    usability.addUsableRange(null, 1, 9);
    Assert.assertEquals("size:2,[MIN,0],[10,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, 21, 29);
    Assert.assertEquals("size:3,[MIN,0],[10,20],[30,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, 41, 49);
    Assert.assertEquals("size:4,[MIN,0],[10,20],[30,40],[50,MAX],", usability.toString());
    System.out.println(usability);
    // cover a range
    usability.addUsableRange(null, 29, 45);
    Assert.assertEquals("size:3,[MIN,0],[10,20],[50,MAX],", usability.toString());
    System.out.println(usability);

    // split a range
    usability.addUsableRange(null, 14, 17);
    Assert.assertEquals("size:4,[MIN,0],[10,13],[18,20],[50,MAX],", usability.toString());
    System.out.println(usability);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SingleLongIndexUsability usability2 = new SingleLongIndexUsability(null);
    usability2.deserialize(in);
    Assert.assertEquals("size:4,[MIN,0],[10,13],[18,20],[50,MAX],", usability2.toString());
  }

  @Test
  public void addUsableRange2() throws IOException {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);
    usability.addUsableRange(null, 1, 19);
    usability.addUsableRange(null, 51, 59);
    usability.addUsableRange(null, 81, 99);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[20,50],[60,80],[100,MAX],", usability.toString());
    // left cover
    usability.addUsableRange(null, 10, 29);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,80],[100,MAX],", usability.toString());
    // right cover
    usability.addUsableRange(null, 71, 99);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,70],[100,MAX],", usability.toString());
    // left cover multiple
    usability.addUsableRange(null, 20, 80);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,0],[100,MAX],", usability.toString());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SingleLongIndexUsability usability2 = new SingleLongIndexUsability(null);
    usability2.deserialize(in);
    Assert.assertEquals("size:2,[MIN,0],[100,MAX],", usability2.toString());
  }

  @Test
  public void minusUsableRange() throws IOException {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);

    // merge with MIN, MAX
    usability.minusUsableRange(null, 1, 19);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());

    // new range is covered by the first range
    usability.addUsableRange(null, 51, 89);
    usability.minusUsableRange(null, 1, 19);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,50],[90,MAX],", usability.toString());

    usability.addUsableRange(null, 101, 259);
    // new range extend node's end time
    usability.minusUsableRange(null, 51, 60);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[90,100],[260,MAX],", usability.toString());

    // new range extend node's start time
    usability.minusUsableRange(null, 80, 90);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[80,100],[260,MAX],", usability.toString());

    // new range is inserted as a individual node [120, 140]
    usability.minusUsableRange(null, 120, 140);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range is totally same as an exist node
    usability.minusUsableRange(null, 120, 140);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range extend the both sides of an exist node
    usability.minusUsableRange(null, 110, 150);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[260,MAX],", usability.toString());

    // an isolate range but the segmentation number reaches the upper bound, thus merge the range with a closer neighbor.
    usability.minusUsableRange(null, 200, 220);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, 50, 90);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, 105, 200);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,100],[105,MAX],", usability.toString());

    // the end
    usability.minusUsableRange(null, 101, 107);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SingleLongIndexUsability usability2 = new SingleLongIndexUsability(null);
    usability2.deserialize(in);
    Assert.assertEquals("size:1,[MIN,MAX],", usability2.toString());
  }

  @Test
  public void subMatchingMerge() {
    SingleLongIndexUsability a = new SingleLongIndexUsability(null, 10);
    a.addUsableRange(null, 31, 39);
    a.addUsableRange(null, 61, 89);
    a.addUsableRange(null, 121, 139);
    a.addUsableRange(null, 161, 169);
    System.out.println(a);

    SingleLongIndexUsability b = new SingleLongIndexUsability(null, 10);
    b.addUsableRange(null, 11, 19);
    b.addUsableRange(null, 51, 79);
    b.addUsableRange(null, 101, 109);
    b.addUsableRange(null, 131, 149);
    b.addUsableRange(null, 181, 209);
    System.out.println(b);
    List<Filter> res = a.getUnusableRangeForSeriesMatching(b);
    Assert.assertEquals(3, res.size());
    Assert.assertEquals(
        "[(time >= -9223372036854775808 && time <= 60), (time >= 80 && time <= 130), (time >= 140 && time <= 9223372036854775807)]",
        res.toString());
    System.out.println(res);

  }
}