//package org.apache.iotdb.db.query.reader.series;
//
//import java.io.IOException;
//import java.util.Set;
//import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
//import org.apache.iotdb.db.index.read.TimeRange;
//import org.apache.iotdb.db.query.context.QueryContext;
//import org.apache.iotdb.db.query.filter.TsFileFilter;
//import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
//import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
//import org.apache.iotdb.tsfile.read.common.Path;
//import org.apache.iotdb.tsfile.read.filter.basic.Filter;
//
//public class SeriesAggregateReaderForIndex extends SeriesAggregateReader {
//
//  public SeriesAggregateReaderForIndex(Path seriesPath,
//      Set<String> allSensors,
//      TSDataType dataType,
//      QueryContext context,
//      QueryDataSource dataSource,
//      Filter timeFilter,
//      Filter valueFilter,
//      TsFileFilter fileFilter) {
//    super(seriesPath, allSensors, dataType, context, dataSource, timeFilter, valueFilter,
//        fileFilter);
//  }
//
////  @Override
////  public boolean canUseCurrentFileStatistics() throws IOException {
////    return !seriesReader.isFileOverlapped()
////        && !seriesReader.currentFileModified();
////  }
//
//
//}
