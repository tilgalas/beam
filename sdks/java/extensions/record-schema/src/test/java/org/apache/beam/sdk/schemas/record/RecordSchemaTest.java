/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class RecordSchemaTest {

  @RunWith(Parameterized.class)
  public static class SchemaMatchesTypeDescriptorTests {
    private final TestRecords.TestCase testCase;

    public SchemaMatchesTypeDescriptorTests(TestRecords.TestCase testCase) {
      this.testCase = testCase;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] testCases() {
      return TestRecords.TestCase.values();
    }

    @Test
    public void testSchemaMatchesTypeDescriptor() {
      Schema actual = new RecordSchema().schemaFor(testCase.typeDescriptor());
      assertNotNull(actual);
      SchemaTestUtils.assertSchemaEquivalent(testCase.schema(), actual);
    }
  }

  @RunWith(Parameterized.class)
  public static class RowConversionsTests<T> {
    private final TestRecords.TestCase.RowAndObject<T> rowAndObject;
    private final TypeDescriptor<T> typeDescriptor;
    private final Function<T, Object> convertForAssertEquals;

    public RowConversionsTests(
        @SuppressWarnings("unused") String testLabel,
        @SuppressWarnings("unused") int testLabelIndex,
        TypeDescriptor<T> typeDescriptor,
        TestRecords.TestCase.RowAndObject<T> rowAndObject,
        Function<T, Object> convertForAssertEquals) {
      this.typeDescriptor = typeDescriptor;
      this.rowAndObject = rowAndObject;
      this.convertForAssertEquals = convertForAssertEquals;
    }

    @Parameterized.Parameters(name = "{0}({1})")
    public static Iterable<Object[]> testCases() {
      return Arrays.stream(TestRecords.TestCase.values())
          .flatMap(
              testCase ->
                  IntStream.range(0, testCase.rowAndObjectList().size())
                      .mapToObj(
                          index ->
                              new Object[] {
                                testCase.name(),
                                index,
                                testCase.typeDescriptor(),
                                testCase.rowAndObjectList().get(index),
                                testCase.convertForAssertEquals()
                              }))
          .toList();
    }

    @Test
    public void testToRowFunction() {
      Row actual =
          new RecordSchema().toRowFunction(typeDescriptor).apply(rowAndObject.javaObject());
      assertEquals(rowAndObject.row(), actual);
    }

    @Test
    public void testFromRowFunction() {
      T actual = new RecordSchema().fromRowFunction(typeDescriptor).apply(rowAndObject.row());
      assertEquals(
          convertForAssertEquals.apply(rowAndObject.javaObject()),
          convertForAssertEquals.apply(actual));
    }
  }

  @RunWith(JUnit4.class)
  public static class OtherTests {
    @Test
    public void testSchemaFieldDescription() {
      Schema actual =
          new RecordSchema().schemaFor(new TypeDescriptor<TestRecords.AnnotatedSimpleRecord>() {});
      assertNotNull(actual);
      assertEquals(
          TestRecords.AnnotatedSimpleRecord.STRING_BUILDER_FIELD_DESCRIPTION,
          actual.getField("stringBuilder").getDescription());
    }

    @Test
    public void testSchemaRegistryFindsASchema() throws Exception {
      Schema schema =
          SchemaRegistry.createDefault()
              .getSchema(new TypeDescriptor<TestRecords.SimpleRecord>() {});
      assertNotNull(schema);
    }
  }
}
