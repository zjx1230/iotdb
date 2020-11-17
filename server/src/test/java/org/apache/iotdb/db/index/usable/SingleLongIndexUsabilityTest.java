package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class SingleLongIndexUsabilityTest {

  @Test
  public void addUsableRange1() {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
    // split range
    usability.addUsableRange(null, newTVList(1, 9));
    Assert.assertEquals("size:2,[MIN,0],[10,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, newTVList(21, 29));
    Assert.assertEquals("size:3,[MIN,0],[10,20],[30,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, newTVList(41, 49));
    Assert.assertEquals("size:4,[MIN,0],[10,20],[30,40],[50,MAX],", usability.toString());
    System.out.println(usability);
    // cover a range
    usability.addUsableRange(null, newTVList(29, 45));
    Assert.assertEquals("size:3,[MIN,0],[10,20],[50,MAX],", usability.toString());
    System.out.println(usability);

    // split a range
    usability.addUsableRange(null, newTVList(14, 17));
    Assert.assertEquals("size:4,[MIN,0],[10,13],[18,20],[50,MAX],", usability.toString());
    System.out.println(usability);
  }

  @Test
  public void addUsableRange2() {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);
    usability.addUsableRange(null, newTVList(1, 19));
    usability.addUsableRange(null, newTVList(51, 59));
    usability.addUsableRange(null, newTVList(81, 99));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[20,50],[60,80],[100,MAX],", usability.toString());
    // left cover
    usability.addUsableRange(null, newTVList(10, 29));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,80],[100,MAX],", usability.toString());
    // right cover
    usability.addUsableRange(null, newTVList(71, 99));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,70],[100,MAX],", usability.toString());
    // left cover multiple
    usability.addUsableRange(null, newTVList(20, 80));
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,0],[100,MAX],", usability.toString());

  }

  private TVList newTVList(long start, long end) {
    TVList res = TVList.newList(TSDataType.INT32);
    assert res != null;
    res.putInt(start, 0);
    res.putInt(end, 1);
    return res;
  }

  @Test
  public void minusUsableRange() {
    SingleLongIndexUsability usability = new SingleLongIndexUsability(null, 4);

    // merge with MIN, MAX
    usability.minusUsableRange(null, newTVList(1, 19));
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());

    // new range is covered by the first range
    usability.addUsableRange(null, newTVList(51, 89));
    usability.minusUsableRange(null, newTVList(1, 19));
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,50],[90,MAX],", usability.toString());

    usability.addUsableRange(null, newTVList(101, 259));
    // new range extend node's end time
    usability.minusUsableRange(null, newTVList(51, 60));
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[90,100],[260,MAX],", usability.toString());

    // new range extend node's start time
    usability.minusUsableRange(null, newTVList(80, 90));
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[80,100],[260,MAX],", usability.toString());

    // new range is inserted as a individual node [120, 140]
    usability.minusUsableRange(null, newTVList(120, 140));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range is totally same as an exist node
    usability.minusUsableRange(null, newTVList(120, 140));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range extend the both sides of an exist node
    usability.minusUsableRange(null, newTVList(110, 150));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[260,MAX],", usability.toString());

    // an isolate range but the segmentation number reaches the upper bound, thus merge the range with a closer neighbor.
    usability.minusUsableRange(null, newTVList(200, 220));
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, newTVList(50, 90));
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, newTVList(105, 200));
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,100],[105,MAX],", usability.toString());

    // the end
    usability.minusUsableRange(null, newTVList(101, 107));
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
  }
}