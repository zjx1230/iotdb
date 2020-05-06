package org.apache.iotdb.db.index.common;

import static org.apache.iotdb.db.index.common.IndexType.ELB;
import static org.apache.iotdb.db.index.common.IndexType.KV_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.PAA;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

public class IndexConstant {


  public static final String INDEXING_SUFFIX = ".indexing";
  public static final String INDEXED_SUFFIX = ".index";

  public static final String INDEX_WINDOW_RANGE = "INDEX_WINDOW_RANGE";
  public static final String INDEX_RANGE_STRATEGY = "INDEX_RANGE_STRATEGY";
  public static final String INDEX_SLIDE_STEP = "INDEX_SLIDE_STEP";

  public static final String DEFAULT_PROP_NAME = "DEFAULT";

  public static final int INDEX_MAP_INIT_RESERVE_SIZE = 5;

  public static final String INDEX_MAGIC = "IoTDBIndex";

  // MBR Index parameters
  public static final String FEATURE_DIM = "FEATURE_DIM";
  public static final String SEED_PICKER = "SEED_PICKER";
  public static final String MAX_ENTRIES = "MAX_ENTRIES";
  public static final String MIN_ENTRIES = "MIN_ENTRIES";


  // Distance
  public static final String DISTANCE = "DISTANCE";
  public static final String L_INFINITY = "L_INFINITY";
  public static final String DEFAULT_DISTANCE = "2";

  public static final String ELB_TYPE = "ELB_TYPE";
  public static final String ELB_TYPE_ELE = "ELE";
  public static final String ELB_TYPE_SEQ = "SEQ";
  public static final String DEFAULT_ELB_TYPE = "SEQ";

  //ELB: calc param
  public static final String ELB_CALC_PARAM = "ELB_CALC_PARAM";
  public static final String DEFAULT_ELB_CALC_PARAM = "SINGLE";
  public static final String ELB_CALC_PARAM_SINGLE = "SINGLE";
  public static final String ELB_THRESHOLD_BASE = "ELB_THRESHOLD_BASE";
  public static final String ELB_THRESHOLD_RATIO = "ELB_THRESHOLD_RATIO";
  public static final double ELB_DEFAULT_THRESHOLD_RATIO = 0.1;


  // index function mapping
  private static Map<IndexType, Set<IndexFunc>> indexSupportFunction = new EnumMap<>(
      IndexType.class);

  static {
    indexSupportFunction.put(NO_INDEX, NO_INDEX.getSupportedFunc());
    indexSupportFunction.put(ELB, ELB.getSupportedFunc());
    indexSupportFunction.put(PAA, PAA.getSupportedFunc());
    indexSupportFunction.put(KV_INDEX, KV_INDEX.getSupportedFunc());
  }

  public static boolean checkIndexQueryValidity(String func, IndexType indexType) {
    IndexFunc indexFunc = IndexFunc.getIndexFunc(func);
    return indexSupportFunction.containsKey(indexType) && indexSupportFunction.get(indexType)
        .contains(indexFunc);
  }

}
