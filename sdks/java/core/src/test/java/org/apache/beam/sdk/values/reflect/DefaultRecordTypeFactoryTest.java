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

package org.apache.beam.sdk.values.reflect;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.reflect.field.FieldValueGetter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link DefaultRecordTypeFactory}.
 */
public class DefaultRecordTypeFactoryTest {

  private static final List<FieldValueGetter> GETTERS_FOR_KNOWN_TYPES = ImmutableList
      .<FieldValueGetter>builder()
      .add(getter("byteGetter", Byte.class))
      .add(getter("integerGetter", Integer.class))
      .add(getter("longGetter", Long.class))
      .add(getter("doubleGetter", Double.class))
      .add(getter("bigDecimalGetter", BigDecimal.class))
      .add(getter("booleanGetter", Boolean.class))
      .add(getter("stringGetter", String.class))
      .build();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testContainsCorrectFields() throws Exception {
    DefaultRecordTypeFactory factory = new DefaultRecordTypeFactory();

    BeamRecordType recordType = factory.createRecordType(GETTERS_FOR_KNOWN_TYPES);

    assertEquals(GETTERS_FOR_KNOWN_TYPES.size(), recordType.getFieldCount());
    assertEquals(
        Arrays.asList(
            "byteGetter",
            "integerGetter",
            "longGetter",
            "doubleGetter",
            "bigDecimalGetter",
            "booleanGetter",
            "stringGetter"),
        recordType.getFieldNames());
  }

  @Test
  public void testContainsCorrectCoders() throws Exception {
    DefaultRecordTypeFactory factory = new DefaultRecordTypeFactory();

    BeamRecordType recordType = factory.createRecordType(GETTERS_FOR_KNOWN_TYPES);

    assertEquals(GETTERS_FOR_KNOWN_TYPES.size(), recordType.getFieldCount());
    assertEquals(
        Arrays.asList(
            ByteCoder.of(),
            VarIntCoder.of(),
            VarLongCoder.of(),
            DoubleCoder.of(),
            BigDecimalCoder.of(),
            BooleanCoder.of(),
            StringUtf8Coder.of()),
        recordType.getRecordCoder().getCoders());
  }

  @Test
  public void testThrowsForUnsupportedTypes() throws Exception {
    thrown.expect(UnsupportedOperationException.class);

    DefaultRecordTypeFactory factory = new DefaultRecordTypeFactory();

    factory.createRecordType(
        Arrays.<FieldValueGetter>asList(getter("dateGetter", Date.class)));
  }

  private static FieldValueGetter<Object> getter(final String fieldName, final Class fieldType) {
    return new FieldValueGetter<Object>() {
      @Override
      public Object get(Object object) {
        return null;
      }

      @Override
      public String name() {
        return fieldName;
      }

      @Override
      public Class type() {
        return fieldType;
      }
    };
  }
}
