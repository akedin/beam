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
package org.apache.beam.sdk.extensions.sql.impl.transform;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlArrayCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlRowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

/**
 * {@link PTransform}s to convert {@link PCollection} elements to {@link Row}s.
 */
public final class FromJson extends PTransform<PCollection<? extends String>, PCollection<Row>> {

  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  private static final Map<SqlTypeCoder, Function<JsonNode, ?>> PRIMITIVE_TYPE_GETTERS =
      ImmutableMap
          .<SqlTypeCoder, Function<JsonNode, ?>>builder()
          .put(SqlTypeCoders.TINYINT, node -> (byte) node.asInt())
          .put(SqlTypeCoders.SMALLINT, node -> (short) node.asInt())
          .put(SqlTypeCoders.INTEGER, node -> node.asInt())
          .put(SqlTypeCoders.BIGINT, node -> node.asLong())
          .put(SqlTypeCoders.FLOAT, node -> (float) node.asDouble())
          .put(SqlTypeCoders.DOUBLE, node -> node.asDouble())
          .put(SqlTypeCoders.BOOLEAN, node -> node.asBoolean())
          .put(SqlTypeCoders.CHAR, node -> node.asText())
          .put(SqlTypeCoders.VARCHAR, node -> node.asText())
          .build();

  private RowType rowType;

  /**
   * Transforms input JSON strings to {@link Row}s.
   *
   * <p>Usage:
   *
   * <pre>{@code
   *   RowType rowType =
   *       RowSqlType
   *           .builder()
   *           ...
   *           .build();
   *
   *   PCollection<String> jsonStrings = ...;
   *   PCollection<Row> rows =
   *       jsonStrings
   *           .apply(FromJson.toRow(rowType));
   * }</pre>
   *
   * <p>All input strings are expected to be JSON objects, not primitive types or arrays.
   *
   * <p>All fields declared in the {@code rowType} must be present in the input JSON strings
   * (explicit nulls are allowed).
   *
   * <p>Nested rows and arrays are supported. Only basic primitive types are supported:
   * integer types, float types, boolean, string types. Date/time/interval types are not supported.
   *
   * <p>If the field value cannot be cast to the row field type, then exception is thrown.
   */
  public static FromJson toRow(RowType rowType) {
    rowType.getRowCoder().getCoders().forEach(coder -> verifyFieldCoderSupported(coder));
    return new FromJson(rowType);
  }

  private FromJson(RowType rowType) {
    this.rowType = rowType;
  }

  private static void verifyFieldCoderSupported(Coder coder) {
    checkArgument(coder instanceof SqlTypeCoder,
                  coder.getClass().getSimpleName()
                  + " is not supported. Only SqlTypeCoders are supported");
    SqlTypeCoder fieldCoder = (SqlTypeCoder) coder;

    if (SqlTypeCoders.isRow(fieldCoder)) {
      RowType rowType = ((SqlRowCoder) fieldCoder).getRowType();
      rowType.getRowCoder().getCoders().forEach(c -> verifyFieldCoderSupported(c));
      return;
    }

    if (SqlTypeCoders.isArray(fieldCoder)) {
      verifyFieldCoderSupported(((SqlArrayCoder) fieldCoder).getElementCoder());
      return;
    }

    if (!PRIMITIVE_TYPE_GETTERS.containsKey(fieldCoder)) {
      throw new IllegalArgumentException(
          "ToRow transform does not support Row fields of type "
          + fieldCoder.getClass().getSimpleName());
    }
  }

  @Override
  public PCollection<Row> expand(PCollection<? extends String> jsonStrings) {
    return
        jsonStrings
            .apply(convertToRow(this.rowType))
            .setCoder(this.rowType.getRowCoder());
  }

  private MapElements<String, Row> convertToRow(RowType rowType) {
    return MapElements.via(new SimpleFunction<String, Row>() {
      @Override
      public Row apply(String jsonString) {
        JsonNode jsonNode = parseJson(jsonString);
        return nodeToRow(jsonNode, "root", rowType);
      }
    });
  }

  private JsonNode parseJson(String jsonString) {
    try {
      return DEFAULT_MAPPER.readTree(jsonString);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to convert JSON object to Row: " + jsonString, e);
    }
  }

  private static Row nodeToRow(
      JsonNode jsonNode,
      String currentFieldName,
      RowType targetRowType) {

    if (!jsonNode.isObject()) {
      throw new IllegalArgumentException(
          "Expected JSON object in " + currentFieldName
          + ". Got " + jsonNode.getNodeType().name());
    }

    return
        IntStream
            .range(0, targetRowType.getFieldCount())
            .mapToObj(fieldIndex -> getFieldValue(jsonNode, targetRowType, fieldIndex))
            .collect(Row.toRow(targetRowType));
  }

  private static Object getFieldValue(
      JsonNode currentRowNode,
      RowType currentRowType,
      int fieldIndex) {

    String fieldName = currentRowType.getFieldName(fieldIndex);
    SqlTypeCoder fieldSqlType = (SqlTypeCoder) currentRowType.getFieldCoder(fieldIndex);
    JsonNode fieldValueNode = currentRowNode.get(fieldName);

    return getFieldValue(fieldValueNode, fieldName, fieldSqlType);
  }

  private static Object getFieldValue(
      JsonNode currentValueNode,
      String currentFieldName,
      SqlTypeCoder currentFieldType) {

    if (currentValueNode == null) {
      throw new IllegalArgumentException(
          "Field " + currentFieldName + " is not present in the JSON node");
    }

    if (currentValueNode.isNull()) {
      return null;
    }

    if (SqlTypeCoders.isArray(currentFieldType)) {
      return
          arrayNodeToList(
              currentValueNode,
              currentFieldName,
              (SqlArrayCoder) currentFieldType);
    }

    if (SqlTypeCoders.isRow(currentFieldType)) {
      return
          nodeToRow(
              currentValueNode,
              currentFieldName,
              ((SqlRowCoder) currentFieldType).getRowType());
    }

    return PRIMITIVE_TYPE_GETTERS.get(currentFieldType).apply(currentValueNode);
  }

  private static Object arrayNodeToList(
      JsonNode currentArrayNode,
      String currentArrayFieldName,
      SqlArrayCoder arrayCoder) {

    if (!currentArrayNode.isArray()) {
      throw new IllegalArgumentException(
          "Expected array for field " + currentArrayFieldName
          + ". Got " + currentArrayNode.getNodeType().name());
    }

    return
        IntStream
            .range(0, currentArrayNode.size())
            .mapToObj(i ->
                          getFieldValue(
                              currentArrayNode.get(i),
                              currentArrayFieldName + "[" + i + "]",
                              arrayCoder.getElementCoder()))
            .collect(toList());
  }
}
