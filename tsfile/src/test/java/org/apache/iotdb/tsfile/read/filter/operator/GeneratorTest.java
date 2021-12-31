package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.codegen.GenerableFilter;
import org.apache.iotdb.tsfile.read.filter.codegen.Generator;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeneratorTest {

  @Test
  public void generateEq() throws CompileException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Eq<Integer> timeFilter = new Eq<>(5, FilterType.TIME_FILTER);

    Filter f = Generator.generate(timeFilter);


    assertTrue(f.satisfy(5L, null));
    assertFalse(f.satisfy(4L, null));
  }

  @Test
  public void generateGt() throws CompileException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Gt<Integer> timeFilter = new Gt<>(5, FilterType.TIME_FILTER);

    Filter f = Generator.generate(timeFilter);


    assertFalse(f.satisfy(5L, null));
    assertTrue(f.satisfy(4L, null));
  }

  @Test
  public void generateAnd() throws CompileException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    GenerableFilter filter = new AndFilter(new Eq<>(5, FilterType.TIME_FILTER), new Eq<>(6, FilterType.VALUE_FILTER));

    Filter f = Generator.generate(filter);


    assertTrue(f.satisfy(5L, 6));
    assertFalse(f.satisfy(4L, 4));
  }

  @Test
  public void generateRegex() throws CompileException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    GenerableFilter filter = new Regexp<>(".*", FilterType.VALUE_FILTER);

    Filter f = Generator.generate(filter);


    assertTrue(f.satisfy(5L, "a"));
  }

  @Test
  public void generateComplex() throws CompileException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    GenerableFilter filter = new AndFilter(
        new AndFilter(
            new Gt<>(50, FilterType.TIME_FILTER),
            new Lt<>(100, FilterType.TIME_FILTER)
        ),
        new OrFilter(
            new AndFilter(
                new Gt<>(4, FilterType.VALUE_FILTER),
                new Lt<>(6, FilterType.VALUE_FILTER)
            ),
            new AndFilter(
                new Gt<>(9, FilterType.VALUE_FILTER),
                new Lt<>(13, FilterType.VALUE_FILTER)
            )
        )
    );

    Filter f = Generator.generate(filter);


    assertTrue(f.satisfy(51L, 5));
    assertFalse(f.satisfy(51L, 4));
    assertFalse(f.satisfy(50L, 5));
  }
}