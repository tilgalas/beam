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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.AutoValueUtils;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({"rawtypes", "nullness"})
public class RecordSchema extends GetterBasedSchemaProvider {

  private static boolean isJavaLangRecord(Class<?> clazz) {
    return clazz.isRecord();
  }

  private static void assertClassIsRecord(Class<?> clazz) {
    Preconditions.checkArgument(
        isJavaLangRecord(clazz), "%s is not a record class", clazz.getCanonicalName());
  }

  /** {@link FieldValueTypeSupplier} that's based on AutoValue getters. */
  @VisibleForTesting
  public static class GetterTypeSupplier implements FieldValueTypeSupplier {
    public static final GetterTypeSupplier INSTANCE = new GetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      Class clazz = typeDescriptor.getRawType();
      assertClassIsRecord(clazz);

      List<Method> methods =
          Arrays.stream(clazz.getRecordComponents())
              .filter(rc -> !rc.isAnnotationPresent(SchemaIgnore.class))
              .map(RecordComponent::getAccessor)
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .toList();

      List<FieldValueTypeInformation> types = Lists.newArrayListWithCapacity(methods.size());
      for (int i = 0; i < methods.size(); ++i) {
        FieldValueTypeInformation.Builder b =
            FieldValueTypeInformation.forGetter(typeDescriptor, methods.get(i), i, false)
                .toBuilder();
        TypeDescriptor resolvedType =
            typeDescriptor.resolveType(methods.get(i).getGenericReturnType());
        b.setType(resolvedType);
        b.setRawType(resolvedType.getRawType());
        FieldValueTypeInformation fieldValueTypeInformation = b.build();
        types.add(fieldValueTypeInformation);
      }
      types.sort(Comparator.comparing(FieldValueTypeInformation::getNumber));
      validateFieldNumbers(types);
      return types;
    }
  }

  private static void validateFieldNumbers(List<FieldValueTypeInformation> types) {
    for (int i = 0; i < types.size(); ++i) {
      FieldValueTypeInformation type = types.get(i);
      @javax.annotation.Nullable Integer number = type.getNumber();
      if (number == null) {
        throw new RuntimeException("Unexpected null number for " + type.getName());
      }
      Preconditions.checkState(
          number == i,
          "Expected field number "
              + i
              + " for field + "
              + type.getName()
              + " instead got "
              + number);
    }
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    assertClassIsRecord(targetTypeDescriptor.getRawType());

    return JavaBeanUtils.getGetters(
        targetTypeDescriptor,
        schema,
        GetterTypeSupplier.INSTANCE,
        new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    assertClassIsRecord(targetTypeDescriptor.getRawType());
    return JavaBeanUtils.getFieldTypes(targetTypeDescriptor, schema, GetterTypeSupplier.INSTANCE);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    assertClassIsRecord(targetTypeDescriptor.getRawType());
    // If a static method is marked with @SchemaCreate, use that.
    Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetTypeDescriptor.getRawType());
    if (annotated != null) {
      return JavaBeanUtils.getStaticCreator(
          targetTypeDescriptor,
          annotated,
          schema,
          GetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    Constructor<?> constructor =
        Optional.ofNullable(ReflectUtils.getAnnotatedConstructor(targetTypeDescriptor.getRawType()))
            .orElseGet(
                () ->
                    Arrays.stream(targetTypeDescriptor.getRawType().getDeclaredConstructors())
                        .filter(c -> !Modifier.isPrivate(c.getModifiers()))
                        .filter(
                            c ->
                                AutoValueUtils.matchConstructor(
                                    targetTypeDescriptor,
                                    c,
                                    GetterTypeSupplier.INSTANCE.get(targetTypeDescriptor, schema)))
                        .findAny()
                        .orElse(null));

    if (constructor != null) {
      return JavaBeanUtils.getConstructorCreator(
          targetTypeDescriptor,
          constructor,
          schema,
          GetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    throw new RuntimeException(
        "Could not find a way to create record class " + targetTypeDescriptor);
  }

  @Override
  public <T> @Nullable Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return isJavaLangRecord(typeDescriptor.getRawType())
        ? StaticSchemaInference.schemaFromClass(typeDescriptor, GetterTypeSupplier.INSTANCE)
        : null;
  }
}
