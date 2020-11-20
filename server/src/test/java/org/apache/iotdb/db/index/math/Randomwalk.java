package org.apache.iotdb.db.index.math;

import java.io.IOException;
import java.util.Random;
import org.apache.iotdb.db.index.IndexTestUtils;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.math.probability.UniformProba;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Created by kangrong on 17/1/8.
 */
public class Randomwalk {

//  private static double randWalkR = 0;
//  private static double randWalkMiu = 1;


  /**
   * 按照random walk model生成数据：
   */
  public static PrimitiveList generateRanWalk(long length, long seed, float R, float miu) {
    PrimitiveList res = PrimitiveList.newList(TSDataType.DOUBLE);
    double lastPoint = R;
    Random r = new Random(seed);
    UniformProba uniform = new UniformProba(miu / 2, -miu / 2, r);
    for (int i = 0; i < length; i++) {
      lastPoint = lastPoint + uniform.getNextRandom();
      res.putDouble(lastPoint);
    }
    return res;
  }

  public static PrimitiveList generateRanWalk(long length) {
    return generateRanWalk(length, 0, 0, 1);
  }

  public static TVList generateRanWalkTVList(long length, long seed, float R, float miu) {
    TVList res = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
    double lastPoint = R;
    Random r = new Random(seed);
    UniformProba uniform = new UniformProba(miu / 2, -miu / 2, r);
    for (int i = 0; i < length; i++) {
      lastPoint = lastPoint + uniform.getNextRandom();
      res.putDouble(i, lastPoint);
    }
    return res;
  }

  public static TVList generateRanWalkTVList(long length) {
    return generateRanWalkTVList(length, 0, 0, 1);
  }

  public static void main(String[] args) {
//    System.out.println(generateRanWalk(10));
    System.out.println(generateRanWalk(10));

    System.out.println(IndexTestUtils.tvListToString(generateRanWalkTVList(10)));
//    System.out.println(generateRanWalkTVList(10));
  }
}
