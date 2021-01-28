package org.apache.iotdb.db.index.usable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * 思考：
 *
 * <p> 1. 区间管理器和IndexProcessor绑定；</p>
 * <p> 2. 接口就是两种添加+一种提取</p>
 * <p> 3. 区分全序列和子序列</p>
 * <p> 4. 全序列</p>
 * <p>     1. 正序的不记录，只记录乱序的序列，而不是区间</p>
 * <p>     2. 无法处理更早期的数据</p>
 * <p>     3. 无法处理晚期的，但不知道新时间是什么</p>
 * <p>     4. 现在的device-sensor和索引的不适配，可以强行扭转，但是仍需讨论</p>
 * <p> 5. 子序列</p>
 * <p>     1. 记录区间：知道最早时间，最晚时间</p>
 * <p>     2. 新加索引只需要更新最晚时间</p>
 * <p>     3. 暂不支持merge，但可以开放接口</p>
 */
public interface IIndexUsable {

  void addUsableRange(PartialPath fullPath, long start, long end);

  void minusUsableRange(PartialPath fullPath, long start, long end);

  /**
   * 获取下面的所有不可用；
   *
   * @return a list of full paths
   */
  Set<PartialPath> getAllUnusableSeriesForWholeMatching();

  /**
   * 基于index给出的后处理范围，结合乱序区间，返回真正的后处理区间
   *
   * 对于subsequence matching，就是区间合并过程
   *
   * 但是，对于不连续的区间，要划分为多个filter，不能连起来
   *
   * @return a time range
   * @param cannotPruned
   */
  List<Filter> mergeUnusableRangeForSubMatching(IIndexUsable cannotPruned);

  void serialize(OutputStream outputStream) throws IOException;

  void deserialize(InputStream inputStream) throws IllegalPathException, IOException;

//  /**
//   * special for ELB, it's not elegant
//   * @param unusableBlocks
//   * @param windowBlocks
//   */
//  void updateELBBlocksForSeriesMatching(PrimitiveList unusableBlocks, List<ELBWindowBlockFeature> windowBlocks);

  IIndexUsable deepCopy();

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexUsable getIndexUsability(PartialPath path) {
      if (path.isFullPath()) {
        return new SingleLongIndexUsability(path);
      } else {
        return new MultiShortIndexUsability(path);
      }
    }

    public static IIndexUsable getIndexUsability(PartialPath path, InputStream inputStream)
        throws IOException, IllegalPathException {
      IIndexUsable res;
      if (path.isFullPath()) {
        res = new SingleLongIndexUsability(path);
        res.deserialize(inputStream);
      } else {
        res = new MultiShortIndexUsability(path);
        res.deserialize(inputStream);
      }
      return res;
    }

  }
}
