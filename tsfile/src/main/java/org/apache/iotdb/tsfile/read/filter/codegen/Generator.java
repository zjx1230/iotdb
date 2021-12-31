package org.apache.iotdb.tsfile.read.filter.codegen;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.FactoryConfigurationError;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Generator {

  private static Logger logger = LoggerFactory.getLogger(Generator.class);

  public static AtomicBoolean active = new AtomicBoolean(true);
  private static AtomicLong count = new AtomicLong(1);

  public static Filter generate(GenerableFilter filter) {
    if (active.get() == false) {
      return filter;
    }
    long start = System.nanoTime();
    if (filter.canGenerate() == false) {
      return filter;
    }

    Expression e = filter.generate();

    // Build a method (public -> modifier 1)
    MethodDeclaration m = Expressions.methodDecl(1, Boolean.TYPE, "satisfy", Arrays.asList(Expressions.parameter(Long.TYPE, "time"), Expressions.parameter(Object.class, "v")), Expressions.block(Expressions.return_(null, e)));

    String s = Expressions.toString(m);

    logger.debug("Generated Filter expression is: \n" + s);

    String className = "DynamicFilter" + count.getAndIncrement();

    String s2 = "import org.apache.iotdb.tsfile.read.filter.basic.Filter;" +
        "" +
        "public " + className + "(Filter delegate) {\nsuper(delegate);\n}\n\n" + s;

    try {
      Scanner scanner = new Scanner("", new StringReader(s2));
      ClassBodyEvaluator ev = new ClassBodyEvaluator(scanner, className, DynamicFilter.class, new Class[]{Filter.class}, null);

      Filter f = (Filter) ev.getClazz().getConstructors()[0].newInstance(filter);

      long end = System.nanoTime();

      logger.debug("Generation time took {} ms", (end - start) / 1e6);

      return f;
    } catch (CompileException | IllegalAccessException | InstantiationException | InvocationTargetException | IOException ex) {
      ex.printStackTrace();
      return filter;
    }
  }
}
