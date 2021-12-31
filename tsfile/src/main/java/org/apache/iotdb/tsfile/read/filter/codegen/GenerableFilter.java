package org.apache.iotdb.tsfile.read.filter.codegen;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public interface GenerableFilter extends Filter {

  boolean canGenerate();

  Expression generate();

}
