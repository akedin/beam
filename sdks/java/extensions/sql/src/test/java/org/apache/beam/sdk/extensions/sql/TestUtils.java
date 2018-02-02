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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Test utilities.
 */
public class TestUtils {
  /**
   * A {@code DoFn} to convert a {@code BeamSqlRow} to a comparable {@code String}.
   */
  public static class BeamSqlRow2StringDoFn extends DoFn<BeamRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().toString());
    }
  }

  /**
   * Convert list of {@code BeamSqlRow} to list of {@code String}.
   */
  public static List<String> beamSqlRows2Strings(List<BeamRecord> rows) {
    List<String> strs = new ArrayList<>();
    for (BeamRecord row : rows) {
      strs.add(row.toString());
    }

    return strs;
  }

  public static RowsBuilder rowsBuilderOf(BeamRecordSqlType type) {
    return RowsBuilder.of(type);
  }

  /**
   * Convenient way to build a list of {@code BeamSqlRow}s.
   *
   * <p>You can use it like this:
   *
   * <pre>{@code
   * TestUtils.RowsBuilder.of(
   *   Types.INTEGER, "order_id",
   *   Types.INTEGER, "sum_site_id",
   *   Types.VARCHAR, "buyer"
   * ).addRows(
   *   1, 3, "james",
   *   2, 5, "bond"
   *   ).getStringRows()
   * }</pre>
   * {@code}
   */
  public static class RowsBuilder {
    private BeamRecordSqlType type;
    private List<BeamRecord> rows = new ArrayList<>();

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   Types.INTEGER, "order_id",
     *   Types.INTEGER, "sum_site_id",
     *   Types.VARCHAR, "buyer"
     * )}</pre>
     *
     * @args pairs of column type and column names.
     */
    public static RowsBuilder of(final Object... args) {
      BeamRecordSqlType beamSQLRowType = buildBeamSqlRowType(args);
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRowType;

      return builder;
    }

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   beamRecordSqlType
     * )}</pre>
     * @beamSQLRowType the record type.
     */
    public static RowsBuilder of(final BeamRecordSqlType beamSQLRowType) {
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRowType;

      return builder;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final Object... args) {
      this.rows.addAll(buildRows(type, Arrays.asList(args)));
      return this;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final List args) {
      this.rows.addAll(buildRows(type, args));
      return this;
    }

    public List<BeamRecord> getRows() {
      return rows;
    }

    public List<String> getStringRows() {
      return beamSqlRows2Strings(rows);
    }

    public PCollectionBuilder getPCollectionBuilder() {
      return
          pCollectionBuilder()
              .withRowType(type)
              .withRows(rows);
    }
  }

  public static PCollectionBuilder pCollectionBuilder() {
    return new PCollectionBuilder();
  }

  static class PCollectionBuilder {
    private BeamRecordType type;
    private List<BeamRecord> rows;
    private String timestampField;
    private Pipeline pipeline;
    private String name;

    public PCollectionBuilder withRowType(BeamRecordType type) {
      this.type = type;
      return this;
    }

    public PCollectionBuilder withRows(List<BeamRecord> rows) {
      this.rows = rows;
      return this;
    }

    /**
     * Event time field, defines watermark.
     */
    public PCollectionBuilder withTimestampField(String timestampField) {
      this.timestampField = timestampField;
      return this;
    }

    public PCollectionBuilder inPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    /**
     * Name of the TestStream PTransform stage.
     */
    public PCollectionBuilder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Builds an unbounded {@link PCollection} in {@link Pipeline}
     * set by {@link #inPipeline(Pipeline)}.
     *
     * <p>If timestamp field was set with {@link #withTimestampField(String)} then
     * watermark will be advanced to the values from that field.
     */
    public PCollection<BeamRecord> buildUnbounded() {
      checkArgument(pipeline != null);
      checkArgument(rows.size() > 0);

      if (type == null) {
        type = rows.get(0).getDataType();
      }

      TestStream.Builder<BeamRecord> values = TestStream.create(type.getRecordCoder());

      for (BeamRecord row : rows) {
        if (timestampField != null) {
          values = values.advanceWatermarkTo(new Instant(row.getDate(timestampField)));
        }

        values = values.addElements(row);
      }

      return name == null
          ? PBegin.in(pipeline).apply(values.advanceWatermarkToInfinity())
          : PBegin.in(pipeline).apply(name, values.advanceWatermarkToInfinity());
    }

    /**
     * Builds a bounded {@link PCollection} in {@link Pipeline}
     * set by {@link #inPipeline(Pipeline)}.
     *
     * <p>If timestamp field was set with {@link #withTimestampField(String)} then
     * watermark will be advanced to the values from that field.
     */
    public PCollection<BeamRecord> buildBounded() {
      checkArgument(pipeline != null);
      checkArgument(rows.size() > 0);

      if (type == null) {
        type = rows.get(0).getDataType();
      }

      Create.Values<BeamRecord> values = Create.of(rows).withCoder(type.getRecordCoder());

      return name == null
          ? PBegin.in(pipeline).apply(values)
          : PBegin.in(pipeline).apply(name, values);
    }
  }

  /**
   * Convenient way to build a {@code BeamSqlRowType}.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildBeamSqlRowType(
   *       Types.BIGINT, "order_id",
   *       Types.INTEGER, "site_id",
   *       Types.DOUBLE, "price",
   *       Types.TIMESTAMP, "order_time"
   *   )
   * }</pre>
   */
  public static BeamRecordSqlType buildBeamSqlRowType(Object... args) {
    List<Integer> types = new ArrayList<>();
    List<String> names = new ArrayList<>();

    for (int i = 0; i < args.length - 1; i += 2) {
      types.add((int) args[i]);
      names.add((String) args[i + 1]);
    }

    return BeamRecordSqlType.create(names, types);
  }

  /**
   * Convenient way to build a {@code BeamSqlRow}s.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildRows(
   *       rowType,
   *       1, 1, 1, // the first row
   *       2, 2, 2, // the second row
   *       ...
   *   )
   * }</pre>
   */
  public static List<BeamRecord> buildRows(BeamRecordSqlType type, List args) {
    List<BeamRecord> rows = new ArrayList<>();
    int fieldCount = type.getFieldCount();

    for (int i = 0; i < args.size(); i += fieldCount) {
      rows.add(new BeamRecord(type, args.subList(i, i + fieldCount)));
    }
    return rows;
  }

  /**
   * Builds a PCollectionTuple.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   pCollectionTupleOf(
   *       "pCollection1_Tag", pCollection1,
   *       "pCollection2_Tag", pCollection2,
   *       ...
   *   )
   * }</pre>
   */
  public static PCollectionTuple pCollectionTupleOf(Object ... args) {
    PCollectionTuple pCollectionTuple = null;

    for (List<Object> taggedPCollection : Lists.partition(Arrays.asList(args), 2)) {
      TupleTag<BeamRecord> tag = new TupleTag<>((String) taggedPCollection.get(0));
      PCollection<BeamRecord> pCollection = (PCollection<BeamRecord>) taggedPCollection.get(1);

      pCollectionTuple = pCollectionTuple == null
          ? PCollectionTuple.of(tag, pCollection)
          : pCollectionTuple.and(tag, pCollection);
    }

    return pCollectionTuple;
  }
}
