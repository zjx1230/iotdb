package org.apache.iotdb.db.index.common;

public class IndexConstant {


  public static final String INDEXING_SUFFIX = ".indexing";
  public static final String INDEXED_SUFFIX = ".index";

  public static final String INDEX_WINDOW_RANGE = "index_window_range";
  public static final String INDEX_RANGE_STRATEGY = "INDEX_RANGE_STRATEGY";
  public static final String INDEX_SLIDE_STEP = "index_slide_step";

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

  //calc param
  public static final String ELB_CALC_PARAM = "ELB_CALC_PARAM";
  public static final String DEFAULT_ELB_CALC_PARAM = "SINGLE";
  public static final String ELB_CALC_PARAM_SINGLE = "SINGLE";
  public static final String ELB_THRESHOLD_BASE = "ELB_THRESHOLD_BASE";
  public static final String ELB_THRESHOLD_RATIO = "ELB_THRESHOLD_RATIO";
  public static final double ELB_DEFAULT_THRESHOLD_RATIO = 0.1;



}
