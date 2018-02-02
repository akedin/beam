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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for nested queries.
 */
public class BeamSqlDslNestedQueriesTest {

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  @Rule
  public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testInUnboundedSubquery() {
    DateTime startTime = new DateTime(2017, 1, 1, 0, 0, 0, 0);

    PCollection<BeamRecord> ordersPCollection =
        unboundedPCollection(
            "orders",
            BeamRecordSqlType.builder()
                .withIntegerField("f_orderId")
                .withIntegerField("f_customerId")
                .withTimestampField("f_timestamp").build(),
            new Object[]{
                1, 11, startTime.plusSeconds(0).toDate(),
                2, 11, startTime.plusSeconds(1).toDate(),
                3, 11, startTime.plusSeconds(2).toDate(),
                4, 22, startTime.plusSeconds(3).toDate(),
                5, 22, startTime.plusSeconds(4).toDate(),
                6, 22, startTime.plusSeconds(6).toDate()
            },
            "f_timestamp")
            .apply("ordersFixedWindow", Window.into(FixedWindows.of(Duration.standardDays(1))));

    PCollection<BeamRecord> customersPCollection =
        unboundedPCollection(
            "customers",
            BeamRecordSqlType.builder()
                .withIntegerField("f_customerId")
                .withVarcharField("f_customerName")
                .withTimestampField("f_timestamp").build(),
            new Object[]{
                11, "customer11", startTime.plusSeconds(0).toDate(),
                22, "customer22", startTime.plusSeconds(1).toDate(),
                33, "customer33", startTime.plusSeconds(2).toDate(),
            },
            "f_timestamp")
            .apply("customersFixedWindow", Window.into(FixedWindows.of(Duration.standardDays(1))));

    String sql =
        "SELECT orders.f_orderId FROM orders WHERE orders.f_customerId IN "
            + "(SELECT customers.f_customerId FROM customers WHERE customers.f_customerId = 22)";

    PCollection<BeamRecord> result =
        TestUtils
            .pCollectionTupleOf(
                "customers", customersPCollection,
                "orders", ordersPCollection)
            .apply("sql", BeamSql.queryMulti(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            rowsWithSingleIntField("f_orderId", Arrays.asList(4, 5, 6)));

    pipeline.run();
  }

  @Test
  public void testInBoundedSubquery() {
    DateTime startTime = new DateTime(2017, 1, 1, 0, 0, 0, 0);

    PCollection<BeamRecord> customersPCollection =
        boundedPCollection(
            "customers",
            BeamRecordSqlType.builder()
                .withIntegerField("f_customerId")
                .withVarcharField("f_customerName")
                .withTimestampField("f_timestamp").build(),
            new Object[]{
                11, "customer11", startTime.plusSeconds(0).toDate(),
                22, "customer22", startTime.plusSeconds(1).toDate(),
                33, "customer33", startTime.plusSeconds(2).toDate(),
            });

    PCollection<BeamRecord> ordersPCollection =
        unboundedPCollection(
            "orders",
            BeamRecordSqlType.builder()
                .withIntegerField("f_orderId")
                .withIntegerField("f_customerId")
                .withTimestampField("f_timestamp").build(),
            new Object[]{
                1, 11, startTime.plusSeconds(0).toDate(),
                2, 11, startTime.plusSeconds(1).toDate(),
                3, 11, startTime.plusSeconds(2).toDate(),
                4, 22, startTime.plusSeconds(3).toDate(),
                5, 22, startTime.plusSeconds(4).toDate(),
                6, 22, startTime.plusSeconds(6).toDate()
            },
            "f_timestamp")
            .apply("ordersFixedWindow", Window.into(FixedWindows.of(Duration.standardDays(1))));

    String sql =
        "SELECT orders.f_orderId FROM orders WHERE orders.f_customerId IN "
            + "(SELECT customers.f_customerId FROM customers WHERE customers.f_customerId = 22)";

    PCollection<BeamRecord> result =
        TestUtils
            .pCollectionTupleOf(
                "customers", customersPCollection,
                "orders", ordersPCollection)
            .apply("sql", BeamSql.queryMulti(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            rowsWithSingleIntField("f_orderId", Arrays.asList(4, 5, 6)));

    pipeline.run();
  }

  private List<BeamRecord> rowsWithSingleIntField(String fieldName, List<Integer> values) {
    return
        TestUtils
            .rowsBuilderOf(BeamRecordSqlType.builder().withIntegerField(fieldName).build())
            .addRows(values)
            .getRows();
  }

  private PCollection<BeamRecord> unboundedPCollection(
      String name,
      BeamRecordSqlType type,
      Object[] rows,
      String timestampField) {
    return
        TestUtils
            .rowsBuilderOf(type)
            .addRows(rows)
            .getPCollectionBuilder()
            .inPipeline(pipeline)
            .withTimestampField(timestampField)
            .withName(name)
            .buildUnbounded();
  }

  private PCollection<BeamRecord> boundedPCollection(
      String name,
      BeamRecordSqlType type,
      Object[] rows) {
    return
        TestUtils
            .rowsBuilderOf(type)
            .addRows(rows)
            .getPCollectionBuilder()
            .inPipeline(pipeline)
            .withName(name)
            .buildBounded();
  }
}
