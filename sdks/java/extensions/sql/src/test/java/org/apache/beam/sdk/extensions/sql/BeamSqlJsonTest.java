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
package org.apache.beam.sdk.extensions.sql;

import static java.util.Arrays.asList;

import org.apache.beam.sdk.extensions.sql.impl.transform.FromJson;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.junit.Rule;
import org.junit.Test;

/**
 * BeamSqlJsonTest.
 */
public class BeamSqlJsonTest {

  private static final String order1 =
      "{ \"orderId\" : 1,\n"
      + "\"person\" : { \"name\" : \"john\", \"personId\" : 12}, \n"
      + "\"items\": [\n"
      + "  { \"sku\" : \"sku1\", \"price\" : 2, \"currency\" : \"USD\", \n"
      + "    \"tags\" : [\"blue\", \"book\"] },\n"
      + "  { \"sku\" : \"sku2\", \"price\" : 13, \"currency\" : \"EUR\", \n"
      + "    \"tags\" : [\"red\", \"car\"] },\n"
      + "  { \"sku\" : \"sku3\", \"price\" : 14, \"currency\" : \"USD\",\n"
      + "    \"tags\" : [\"green\", \"box\"]}\n"
      + "]}\n";

  private static final String order2 =
      "{ \"orderId\" : 2,\n"
      + "\"person\" : { \"name\" : \"jane\", \"personId\" : 2},\n"
      + "\"items\": [\n"
      + "  { \"sku\" : \"sku3\", \"price\" : 1, \"currency\" : \"EUR\", \n"
      + "    \"tags\" : [\"red\", \"car\"] },\n"
      + "  { \"sku\" : \"sku1\", \"price\" : 4, \"currency\" : \"USD\",\n"
      + "    \"tags\" : [\"green\", \"box\"]}\n"
      + "]}\n";

  private static final String order3 =
      "{ \"orderId\" : 3,\n"
      + "\"person\" : { \"name\" : \"john\", \"personId\" : 12},\n"
      + "\"items\": [\n"
      + "  { \"sku\" : \"sku2\", \"price\" : 5, \"currency\" : \"USD\", \n"
      + "    \"tags\" : [\"blue\", \"book\"] }\n"
      + "]}\n";

  private static final String order4 =
      "{ \"orderId\" : 4,\n"
      + "\"person\" : { \"name\" : \"jane\", \"personId\" : 2},\n"
      + "\"items\": [\n"
      + "  { \"sku\" : \"sku1\", \"price\" : 6, \"currency\" : \"EUR\", \n"
      + "    \"tags\" : [\"red\", \"car\"] },\n"
      + "  { \"sku\" : \"sku5\", \"price\" : 2, \"currency\" : \"USD\",\n"
      + "    \"tags\" : [\"green\", \"box\"]}\n"
      + "]}\n";

  private static final String order5 =
      "{ \"orderId\" : 5,\n"
      + "\"person\" : { \"name\" : \"foobar\", \"personId\" : 3},\n"
      + "\"items\": [\n"
      + "  { \"sku\" : \"sku17\", \"price\" : 2, \"currency\" : \"USD\",\n"
      + "    \"tags\" : [\"clear\", \"circle\"]}\n"
      + "]}\n";

  private static final RowType ITEM_TYPE =
      RowSqlType
          .builder()
          .withVarcharField("sku")
          .withIntegerField("price")
          .withVarcharField("currency")
          .withArrayField("tags", SqlTypeCoders.VARCHAR)
          .build();

  private static final RowType PERSON_TYPE =
      RowSqlType
          .builder()
          .withVarcharField("name")
          .withIntegerField("personId")
          .build();

  private static final RowType ORDER_TYPE =
      RowSqlType
          .builder()
          .withIntegerField("orderId")
          .withRowField("person", PERSON_TYPE)
          .withArrayField("items", SqlTypeCoders.rowOf(ITEM_TYPE))
          .build();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSelectTopLevelPrimitiveField() {
    RowType outputType = RowSqlType.builder().withIntegerField("orderId").build();

    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query("SELECT orderId FROM PCOLLECTION"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(outputType).addValues(1).build(),
            Row.withRowType(outputType).addValues(2).build(),
            Row.withRowType(outputType).addValues(3).build(),
            Row.withRowType(outputType).addValues(4).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectTopLevelObjectField() {
    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4, order5));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query("SELECT person FROM PCOLLECTION"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(PERSON_TYPE).addValues("john", 12).build(),
            Row.withRowType(PERSON_TYPE).addValues("jane", 2).build(),
            Row.withRowType(PERSON_TYPE).addValues("john", 12).build(),
            Row.withRowType(PERSON_TYPE).addValues("jane", 2).build(),
            Row.withRowType(PERSON_TYPE).addValues("foobar", 3).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectTopLevelArrayField() {
    RowType outputType = RowSqlType.builder().withArrayField("array", ITEM_TYPE).build();

    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4, order5));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query("SELECT items FROM PCOLLECTION"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(outputType).addArray(
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku1", 2, "USD", asList("blue", "book")).build(),
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku2", 13, "EUR", asList("red", "car")).build(),
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku3", 14, "USD", asList("green", "box")).build()).build(),
            Row.withRowType(outputType).addArray(
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku3", 1, "EUR", asList("red", "car")).build(),
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku1", 4, "USD", asList("green", "box")).build()).build(),
            Row.withRowType(outputType).addArray(
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku2", 5, "USD", asList("blue", "book")).build()).build(),
            Row.withRowType(outputType).addArray(
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku1", 6, "EUR", asList("red", "car")).build(),
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku5", 2, "USD", asList("green", "box")).build()).build(),
            Row.withRowType(outputType).addArray(
                Row.withRowType(ITEM_TYPE)
                   .addValues("sku17", 2, "USD", asList("clear", "circle")).build()).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectFieldInNestedObject() {
    RowType outputType = RowSqlType.builder().withVarcharField("name").build();

    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4, order5));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query("SELECT PCOLLECTION.person.name FROM PCOLLECTION"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(outputType).addValues("john").build(),
            Row.withRowType(outputType).addValues("jane").build(),
            Row.withRowType(outputType).addValues("john").build(),
            Row.withRowType(outputType).addValues("jane").build(),
            Row.withRowType(outputType).addValues("foobar").build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectElementFromTopLevelArrayField() {
    RowType outputType = RowSqlType.builder().withArrayField("array", ITEM_TYPE).build();

    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4, order5));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query("SELECT `PCOLLECTION`.`items`[0] FROM PCOLLECTION"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(ITEM_TYPE).addValues("sku1", 2, "USD", asList("blue", "book")).build(),
            Row.withRowType(ITEM_TYPE).addValues("sku3", 1, "EUR", asList("red", "car")).build(),
            Row.withRowType(ITEM_TYPE).addValues("sku2", 5, "USD", asList("blue", "book")).build(),
            Row.withRowType(ITEM_TYPE).addValues("sku1", 6, "EUR", asList("red", "car")).build(),
            Row.withRowType(ITEM_TYPE).addValues("sku17", 2, "USD", asList("clear", "circle")).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testBasicAggregationOnTopLevelField() {
    RowType outputType = RowSqlType.builder().withBigIntField("count").build();

    PCollection<String> input =
        pipeline
            .apply(Create.of(order1, order2, order3, order4, order5));

    PCollection<Row> output =
        input
            .apply(FromJson.toRow(ORDER_TYPE))
            .apply(BeamSql.query(
                "SELECT COUNT(PCOLLECTION.person.name) "
                + "FROM PCOLLECTION "
                + "GROUP BY PCOLLECTION.person.name"));

    PAssert
        .that(output)
        .containsInAnyOrder(
            Row.withRowType(outputType).addValues(2L).build(),
            Row.withRowType(outputType).addValues(2L).build(),
            Row.withRowType(outputType).addValues(1L).build());

    pipeline.run().waitUntilFinish();
  }
}
