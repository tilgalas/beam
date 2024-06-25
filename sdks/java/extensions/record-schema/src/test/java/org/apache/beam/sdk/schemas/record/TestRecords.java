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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class TestRecords {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(StandardCharsets.UTF_8);

  public interface SimpleRecordInterface {
    String str();

    byte aByte();

    short aShort();

    int anInt();

    long aLong();

    boolean aBoolean();

    DateTime dateTime();

    Instant instant();

    byte[] bytes();

    ByteBuffer byteBuffer();

    BigDecimal bigDecimal();

    StringBuilder stringBuilder();
  }

  @DefaultSchema(RecordSchema.class)
  public record SimpleRecord(
      String str,
      byte aByte,
      short aShort,
      int anInt,
      long aLong,
      boolean aBoolean,
      DateTime dateTime,
      Instant instant,
      byte[] bytes,
      ByteBuffer byteBuffer,
      BigDecimal bigDecimal,
      StringBuilder stringBuilder)
      implements SimpleRecordInterface {

    public SimpleRecordForAssertEquals convertForAssertEquals() {
      return SimpleRecordForAssertEquals.fromSimpleRecord(this);
    }

    public static SimpleRecord create(String name) {
      return new SimpleRecord(
          name,
          (byte) 1,
          (short) 2,
          3,
          4L,
          true,
          DATE,
          DATE.toInstant(),
          BYTE_ARRAY,
          ByteBuffer.wrap(BYTE_ARRAY),
          BigDecimal.ONE,
          new StringBuilder(name + "Builder"));
    }
  }

  public static Row createSimpleRow(String name) {
    return addSimpleRowValues(name).apply(Row.withSchema(SIMPLE_RECORD_SCHEMA)).build();
  }

  public static Function<Row.Builder, Row.Builder> addSimpleRowValues(String name) {
    return builder ->
        builder.addValues(
            name,
            (byte) 1,
            (short) 2,
            3,
            4L,
            true,
            DATE.toInstant(),
            DATE.toInstant(),
            BYTE_ARRAY,
            BYTE_ARRAY,
            BigDecimal.ONE,
            name + "Builder");
  }

  public static Schema SIMPLE_RECORD_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addByteField("aByte")
          .addInt16Field("aShort")
          .addInt32Field("anInt")
          .addInt64Field("aLong")
          .addBooleanField("aBoolean")
          .addDateTimeField("dateTime")
          .addDateTimeField("instant")
          .addByteArrayField("bytes")
          .addByteArrayField("byteBuffer")
          .addDecimalField("bigDecimal")
          .addStringField("stringBuilder")
          .build();

  public record SimpleRecordForAssertEquals(
      String str,
      byte aByte,
      short aShort,
      int anInt,
      long aLong,
      boolean aBoolean,
      DateTime dateTime,
      Instant instant,
      ByteBuffer bytes,
      ByteBuffer byteBuffer,
      BigDecimal bigDecimal,
      String stringBuilder) {
    public static SimpleRecordForAssertEquals fromSimpleRecord(SimpleRecordInterface simpleRecord) {
      return new SimpleRecordForAssertEquals(
          simpleRecord.str(),
          simpleRecord.aByte(),
          simpleRecord.aShort(),
          simpleRecord.anInt(),
          simpleRecord.aLong(),
          simpleRecord.aBoolean(),
          simpleRecord.dateTime(),
          simpleRecord.instant(),
          ByteBuffer.wrap(simpleRecord.bytes()),
          simpleRecord.byteBuffer(),
          simpleRecord.bigDecimal(),
          simpleRecord.stringBuilder().toString());
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record NullableRecord(@Nullable String str, int anInt) {}

  @DefaultSchema(RecordSchema.class)
  public record AnnotatedSimpleRecord(
      @SchemaFieldNumber("0") String str,
      @SchemaFieldName("aByteAnnotated") @SchemaFieldNumber("2") byte aByte,
      @SchemaFieldName("aShortAnnotated") @SchemaFieldNumber("1") short aShort,
      @SchemaFieldNumber("3") int anInt,
      @SchemaFieldNumber("4") long aLong,
      @SchemaFieldNumber("5") boolean aBoolean,
      @SchemaFieldNumber("6") DateTime dateTime,
      @SchemaFieldNumber("7") Instant instant,
      @SchemaFieldNumber("8") byte[] bytes,
      @SchemaFieldNumber("9") ByteBuffer byteBuffer,
      @SchemaFieldNumber("10") BigDecimal bigDecimal,
      @SchemaFieldDescription(STRING_BUILDER_FIELD_DESCRIPTION) @SchemaFieldNumber("11")
          StringBuilder stringBuilder,
      @SchemaIgnore String ignoredField)
      implements SimpleRecordInterface {

    public static final String STRING_BUILDER_FIELD_DESCRIPTION =
        "a string builder field, which corresponds to a string row field";

    @SchemaCreate
    @SuppressWarnings("unused")
    public static AnnotatedSimpleRecord create(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
        ByteBuffer byteBuffer,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {

      return new AnnotatedSimpleRecord(
          str,
          aByte,
          aShort,
          anInt,
          aLong,
          aBoolean,
          dateTime,
          instant,
          bytes,
          byteBuffer,
          bigDecimal,
          stringBuilder,
          "ignored in creator");
    }

    SimpleRecordForAssertEquals convertForAssertEquals() {
      return SimpleRecordForAssertEquals.fromSimpleRecord(this);
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record ArrayOfByteArrays(ByteBuffer[] byteBuffers) {
    public record ForAssertEquals(List<ByteBuffer> byteBuffers) {}

    public ForAssertEquals convertForAssertEquals() {
      return new ForAssertEquals(Arrays.stream(byteBuffers).toList());
    }
  }

  @DefaultSchema(RecordSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  public record CaseFormatRecord(
      String user,
      int ageInYears,
      @SchemaCaseFormat(CaseFormat.UPPER_CAMEL) boolean knowsJavascript) {}

  @DefaultSchema(RecordSchema.class)
  public record IterableRecord(Iterable<String> strings) {}

  @DefaultSchema(RecordSchema.class)
  public record NestedArrayRecord(SimpleRecord... records) {
    public record ForAssertEquals(List<SimpleRecordForAssertEquals> records) {}

    public ForAssertEquals convertForAssertEquals() {
      return new ForAssertEquals(
          Arrays.stream(records).map(SimpleRecord::convertForAssertEquals).toList());
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record NestedArraysRecord(List<List<String>> lists) {}

  @DefaultSchema(RecordSchema.class)
  public record NestedRecord(SimpleRecord nested) {
    public record ForAssertEquals(SimpleRecordForAssertEquals nested) {}

    public ForAssertEquals convertForAssertEquals() {
      return new ForAssertEquals(nested.convertForAssertEquals());
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record NestedMapRecord(Map<String, SimpleRecord> map) {
    public record ForAssertEquals(Map<String, SimpleRecordForAssertEquals> map) {}

    public ForAssertEquals convertForAssertEquals() {
      return new ForAssertEquals(
          map.entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, e -> e.getValue().convertForAssertEquals())));
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record PrimitiveArrayRecord(
      // Test every type of array parameter supported.
      List<String> strings, int[] integers, Long[] longs) {

    public record ForAssertEquals(List<String> strings, List<Integer> integers, List<Long> longs) {}

    public ForAssertEquals convertForAssertEquals() {
      return new ForAssertEquals(
          strings, Arrays.stream(integers).boxed().toList(), Arrays.stream(longs).toList());
    }
  }

  @DefaultSchema(RecordSchema.class)
  public record PrimitiveMapRecord(Map<String, Integer> map) {}

  @DefaultSchema(RecordSchema.class)
  public record GenericRecord<T>(T t) {}

  @DefaultSchema(RecordSchema.class)
  private record GenericRecordWithCreator<T>(@SuppressWarnings("unused") T t) {

    @SuppressWarnings("unused")
    @SchemaCreate
    public static <T> GenericRecordWithCreator<T> create(T t) {
      return new GenericRecordWithCreator<>(t);
    }
  }

  @SuppressWarnings("unused")
  @DefaultSchema(RecordSchema.class)
  private record GenericRecordWithAnnotatedCtor<T>(T t, String str) {

    @SchemaCreate
    public GenericRecordWithAnnotatedCtor(T t) {
      this(t, "default string");
    }
  }

  public enum TestCase {
    // add an enum value here, and it will be run as a test case to verify that the schema
    // corresponds to the type, and that the row can be converted to a java object and vice-versa
    SIMPLE_RECORD(
        new TestData<>(
            TestData.<SimpleRecord>specBuilder()
                .setSchema(SIMPLE_RECORD_SCHEMA)
                .addRowAndObjectBuilder(addSimpleRowValues("string"), SimpleRecord.create("string"))
                .setConvertForAssertEquals(SimpleRecord::convertForAssertEquals)) {}),

    ANNOTATED_SIMPLE_RECORD(
        new TestData<>(
            TestData.<AnnotatedSimpleRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addStringField("str")
                        .addInt16Field("aShortAnnotated")
                        .addByteField("aByteAnnotated")
                        .addInt32Field("anInt")
                        .addInt64Field("aLong")
                        .addBooleanField("aBoolean")
                        .addDateTimeField("dateTime")
                        .addDateTimeField("instant")
                        .addByteArrayField("bytes")
                        .addByteArrayField("byteBuffer")
                        .addDecimalField("bigDecimal")
                        .addField(
                            Schema.Field.of("stringBuilder", Schema.FieldType.STRING)
                                .withDescription(
                                    AnnotatedSimpleRecord.STRING_BUILDER_FIELD_DESCRIPTION)))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addValues(
                            "string",
                            (short) 2,
                            (byte) 1,
                            3,
                            4L,
                            true,
                            DATE.toInstant(),
                            DATE.toInstant(),
                            BYTE_ARRAY,
                            BYTE_ARRAY,
                            BigDecimal.ONE,
                            "string"),
                    new AnnotatedSimpleRecord(
                        "string",
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        true,
                        DATE,
                        DATE.toInstant(),
                        BYTE_ARRAY,
                        ByteBuffer.wrap(BYTE_ARRAY),
                        BigDecimal.ONE,
                        new StringBuilder("string"),
                        "ignored"))
                .setConvertForAssertEquals(AnnotatedSimpleRecord::convertForAssertEquals)) {}),
    NULLABLE_RECORD(
        new TestData<>(
            TestData.<NullableRecord>specBuilder()
                .setSchema(Schema.builder().addNullableStringField("str").addInt32Field("anInt"))
                .addRowAndObjectBuilder(
                    builder -> builder.addValues(null, 42), new NullableRecord(null, 42))
                .addRowAndObjectBuilder(
                    builder -> builder.addValues("string", 42),
                    new NullableRecord("string", 42))) {}),
    ARRAY_OF_BYTE_ARRAY_RECORD(
        new TestData<>(
            TestData.<ArrayOfByteArrays>specBuilder()
                .setSchema(Schema.builder().addArrayField("byteBuffers", Schema.FieldType.BYTES))
                .addRowAndObjectBuilder(
                    builder -> builder.addArray(BYTE_ARRAY, BYTE_ARRAY),
                    new ArrayOfByteArrays(
                        new ByteBuffer[] {
                          ByteBuffer.wrap(BYTE_ARRAY), ByteBuffer.wrap(BYTE_ARRAY)
                        }))
                .setConvertForAssertEquals(ArrayOfByteArrays::convertForAssertEquals)) {}),
    CASE_FORMAT_RECORD(
        new TestData<>(
            TestData.<CaseFormatRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addStringField("user")
                        .addInt32Field("age_in_years")
                        .addBooleanField("KnowsJavascript"))
                .addRowAndObjectBuilder(
                    builder -> builder.addValues("user1", 42, true),
                    new CaseFormatRecord("user1", 42, true))) {}),
    ITERABLE_RECORD(
        new TestData<>(
            TestData.<IterableRecord>specBuilder()
                .setSchema(Schema.builder().addIterableField("strings", Schema.FieldType.STRING))
                .addRowAndObjectBuilder(
                    builder -> builder.addIterable(ImmutableList.of("str1", "str2")),
                    new IterableRecord(ImmutableList.of("str1", "str2")))) {}),
    NESTED_ARRAY_RECORD(
        new TestData<>(
            TestData.<NestedArrayRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addArrayField("records", Schema.FieldType.row(SIMPLE_RECORD.schema())))
                .addRowAndObjectBuilder(
                    builder -> builder.addArray(createSimpleRow("1"), createSimpleRow("2")),
                    new NestedArrayRecord(SimpleRecord.create("1"), SimpleRecord.create("2")))
                .setConvertForAssertEquals(NestedArrayRecord::convertForAssertEquals)) {}),

    NESTED_ARRAYS_RECORD(
        new TestData<>(
            TestData.<NestedArraysRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addArrayField("lists", Schema.FieldType.array(Schema.FieldType.STRING)))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addArray(
                            ImmutableList.of("str1", "str2"), ImmutableList.of("str3", "str4")),
                    new NestedArraysRecord(
                        ImmutableList.of(
                            ImmutableList.of("str1", "str2"),
                            ImmutableList.of("str3", "str4"))))) {}),
    NESTED_RECORD(
        new TestData<>(
            TestData.<NestedRecord>specBuilder()
                .setSchema(Schema.builder().addRowField("nested", SIMPLE_RECORD_SCHEMA))
                .addRowAndObjectBuilder(
                    builder -> builder.addValue(createSimpleRow("string")),
                    new NestedRecord(SimpleRecord.create("string")))
                .setConvertForAssertEquals(NestedRecord::convertForAssertEquals)) {}),
    NESTED_MAP_RECORD(
        new TestData<>(
            TestData.<NestedMapRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addMapField(
                            "map",
                            Schema.FieldType.STRING,
                            Schema.FieldType.row(SIMPLE_RECORD.schema())))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addValue(
                            ImmutableMap.<String, Row>builder()
                                .put("s1", createSimpleRow("s1"))
                                .put("s2", createSimpleRow("s2"))
                                .put("s3", createSimpleRow("s3"))
                                .build()),
                    new NestedMapRecord(
                        Stream.of("s1", "s2", "s3")
                            .collect(Collectors.toMap(k -> k, SimpleRecord::create))))
                .setConvertForAssertEquals(NestedMapRecord::convertForAssertEquals)) {}),
    PRIMITIVE_ARRAY_RECORD(
        new TestData<>(
            TestData.<PrimitiveArrayRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addArrayField("strings", Schema.FieldType.STRING)
                        .addArrayField("integers", Schema.FieldType.INT32)
                        .addArrayField("longs", Schema.FieldType.INT64))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addArray("s1", "s2", "s3").addArray(1, 2, 3).addArray(1L, 2L, 3L),
                    new PrimitiveArrayRecord(
                        ImmutableList.of("s1", "s2", "s3"),
                        new int[] {1, 2, 3},
                        new Long[] {1L, 2L, 3L}))
                .setConvertForAssertEquals(PrimitiveArrayRecord::convertForAssertEquals)) {}),
    PRIMITIVE_MAP_RECORD(
        new TestData<>(
            TestData.<PrimitiveMapRecord>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.INT32))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addValue(
                            ImmutableMap.<String, Integer>builder()
                                .put("k1", 1)
                                .put("k2", 2)
                                .put("k3", 3)
                                .build()),
                    new PrimitiveMapRecord(
                        ImmutableMap.<String, Integer>builder()
                            .put("k1", 1)
                            .put("k2", 2)
                            .put("k3", 3)
                            .build()))) {}),

    GENERIC_RECORD_LONG(
        new TestData<>(
            TestData.<GenericRecord<Long>>specBuilder()
                .setSchema(Schema.builder().addInt64Field("t"))
                .addRowAndObjectBuilder(
                    builder -> builder.addValue(42L), new GenericRecord<>(42L))) {}),
    GENERIC_RECORD_NESTED(
        new TestData<>(
            TestData.<GenericRecord<GenericRecord<Long>>>specBuilder()
                .setSchema(Schema.builder().addRowField("t", GENERIC_RECORD_LONG.schema()))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addValue(
                            Row.withSchema(GENERIC_RECORD_LONG.schema()).addValue(42L).build()),
                    new GenericRecord<>(new GenericRecord<>(42L)))) {}),
    GENERIC_RECORD_WITH_CREATOR(
        new TestData<>(
            TestData.<GenericRecordWithCreator<Long>>specBuilder()
                .setSchema(Schema.builder().addInt64Field("t"))
                .addRowAndObjectBuilder(
                    builder -> builder.addValue(42L), GenericRecordWithCreator.create(42L))) {}),
    GENERIC_RECORD_NESTED_MAP(
        new TestData<>(
            TestData.<GenericRecord<Map<String, GenericRecord<Long>>>>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addMapField(
                            "t",
                            Schema.FieldType.STRING,
                            Schema.FieldType.row(Schema.builder().addInt64Field("t").build())))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addValue(
                            ImmutableMap.<String, Row>builder()
                                .put(
                                    "k1",
                                    Row.withSchema(GENERIC_RECORD_LONG.schema())
                                        .addValue(1L)
                                        .build())
                                .put(
                                    "k2",
                                    Row.withSchema(GENERIC_RECORD_LONG.schema())
                                        .addValue(2L)
                                        .build())
                                .build()),
                    new GenericRecord<>(
                        ImmutableMap.<String, GenericRecord<Long>>builder()
                            .put("k1", new GenericRecord<>(1L))
                            .put("k2", new GenericRecord<>(2L))
                            .build()))) {}),
    @SuppressWarnings("unchecked")
    GENERIC_RECORD_NESTED_ARRAY(
        new TestData<>(
            TestData.<GenericRecord<GenericRecord<Long>[]>>specBuilder()
                .setSchema(
                    Schema.builder()
                        .addArrayField("t", Schema.FieldType.row(GENERIC_RECORD_LONG.schema())))
                .addRowAndObjectBuilder(
                    builder ->
                        builder.addArray(
                            Row.withSchema(GENERIC_RECORD_LONG.schema()).addValue(1L).build(),
                            Row.withSchema(GENERIC_RECORD_LONG.schema()).addValue(2L).build(),
                            Row.withSchema(GENERIC_RECORD_LONG.schema()).addValue(3L).build()),
                    new GenericRecord<>(
                        (GenericRecord<Long>[])
                            new GenericRecord<?>[] {
                              new GenericRecord<>(1L),
                              new GenericRecord<>(2L),
                              new GenericRecord<>(3L)
                            }))
                .setConvertForAssertEquals(
                    genericRecord ->
                        new GenericRecord<>(Arrays.stream(genericRecord.t()).toList()))) {}),
    GENERIC_RECORD_WITH_ANNOTATED_CTOR(
        new TestData<>(
            TestData.<GenericRecordWithAnnotatedCtor<Long>>specBuilder()
                .setSchema(Schema.builder().addInt64Field("t").addStringField("str").build())
                .addRowAndObjectBuilder(
                    builder -> builder.addValue(64L).addValue("default string"),
                    new GenericRecordWithAnnotatedCtor<>(64L))) {}),
    ;

    public record RowAndObject<T>(Row row, T javaObject) {}

    /**
     * A single row-object pair builder used in to/from row functions tests.
     *
     * @param buildRow a function that should manipulate the provided builder object to build a row
     *     corresponding to the {@code javaObject} parameter
     * @param javaObject an object corresponding to the {@code buildRow} parameter
     * @param <T> type of the {@code javaObject}
     */
    public record RowAndObjectBuilder<T>(
        Function<Row.Builder, Row.Builder> buildRow, T javaObject) {

      public RowAndObject<T> build(Schema schema) {
        return new RowAndObject<>(buildRow.apply(Row.withSchema(schema)).build(), javaObject);
      }
    }

    /**
     * Captures the java type (possibly generic) that will correspond to a schema inferred for it in
     * tests, must be constructed by creating an anonymous subclass, similarly to how {@link
     * TypeDescriptor} is constructed, eg {@code new TestData<MyType<Long>>(spec){};}.
     *
     * @param <T> java type to use in test
     */
    @Immutable
    abstract static class TestData<T> {

      public static <T> Spec.Builder<T> specBuilder() {
        return Spec.builder();
      }

      @AutoValue
      @Immutable
      abstract static class Spec<T> {
        public abstract Schema getSchema();

        public abstract List<RowAndObjectBuilder<T>> getRowAndObjectBuilderList();

        /**
         * A converter function used when a tested object cannot be simply compared using its {@code
         * equals} method, for example because it contains an array field.
         *
         * @return a function that converts the tested object just before running an assertEquals
         *     test on it
         */
        public abstract Function<T, Object> getConvertForAssertEquals();

        public static <T> Spec.Builder<T> builder() {
          return new AutoValue_TestRecords_TestCase_TestData_Spec.Builder<T>()
              .setConvertForAssertEquals(x -> x);
        }

        @AutoValue.Builder
        abstract static class Builder<T> {

          public abstract Builder<T> setSchema(Schema schema);

          public Builder<T> setSchema(Schema.Builder schemaBuilder) {
            return setSchema(schemaBuilder.build());
          }

          public abstract Builder<T> setRowAndObjectBuilderList(
              List<RowAndObjectBuilder<T>> rowAndObjectBuilderList);

          public Builder<T> addRowAndObjectBuilder(
              Function<Row.Builder, Row.Builder> buildRow, T javaObject) {
            return addRowAndObjectBuilder(new RowAndObjectBuilder<>(buildRow, javaObject));
          }

          public Builder<T> addRowAndObjectBuilder(RowAndObjectBuilder<T> rowAndObjectBuilder) {
            rowAndObjectBuilderListBuilder().add(rowAndObjectBuilder);
            return this;
          }

          public abstract ImmutableList.Builder<RowAndObjectBuilder<T>>
              rowAndObjectBuilderListBuilder();

          public abstract Builder<T> setConvertForAssertEquals(
              Function<T, Object> convertForAssertEquals);

          public abstract Spec<T> build();
        }
      }

      private final Spec<T> spec;

      @SuppressWarnings("Immutable")
      private final TypeDescriptor<T> typeDescriptor;

      @SuppressWarnings("Immutable")
      private final List<RowAndObject<T>> rowAndObjectList;

      private TestData(Spec.Builder<T> specBuilder) {
        this(specBuilder.build());
      }

      private TestData(Spec<T> spec) {
        this.spec = spec;
        this.typeDescriptor = new TypeDescriptor<>(getClass()) {};
        this.rowAndObjectList =
            spec.getRowAndObjectBuilderList().stream()
                .map(rowAndObjectBuilder -> rowAndObjectBuilder.build(spec.getSchema()))
                .toList();
      }

      public TypeDescriptor<T> typeDescriptor() {
        return typeDescriptor;
      }

      public Schema schema() {
        return spec.getSchema();
      }

      public List<RowAndObject<T>> rowAndObjectList() {
        return rowAndObjectList;
      }

      public Function<T, Object> convertForAssertEquals() {
        return spec.getConvertForAssertEquals();
      }
    }

    private final TestData<?> testData;

    TestCase(TestData<?> testData) {
      this.testData = testData;
    }

    public Schema schema() {
      return testData.schema();
    }

    public TypeDescriptor<?> typeDescriptor() {
      return testData.typeDescriptor();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<RowAndObject<?>> rowAndObjectList() {
      return (List) testData.rowAndObjectList();
    }

    public Function<?, Object> convertForAssertEquals() {
      return testData.convertForAssertEquals();
    }
  }
}
