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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link FromJson}.
 */
public class FromJsonTest implements Serializable {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static final RowType SUBPOJO_ROW_TYPE =
      RowSqlType
        .builder()
        .withVarcharField("description")
        .withDoubleField("progress")
        .build();

  private static final RowType POJO_ROW_TYPE =
      RowSqlType
        .builder()
        .withIntegerField("id")
        .withVarcharField("name")
        .withArrayField("colors", SqlTypeCoders.VARCHAR)
        .withRowField("subPojo", SUBPOJO_ROW_TYPE)
        .withArrayField("subPojos", SUBPOJO_ROW_TYPE)
        .build();

  /** Test pojo. */
  public static class Pojo implements Serializable {
    private Integer id;
    private String name;
    private SubPojo subPojo;
    private List<SubPojo> subPojos;
    private List<String> colors;

    public Pojo(Integer id,
                String name,
                SubPojo subPojo,
                List<SubPojo> subPojos,
                List<String> colors) {
      this.id = id;
      this.name = name;
      this.subPojo = subPojo;
      this.subPojos = subPojos;
      this.colors = colors;
    }

    public Integer getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public SubPojo getSubPojo() {
      return subPojo;
    }

    public List<SubPojo> getSubPojos() {
      return subPojos;
    }

    public List<String> getColors() {
      return colors;
    }
  }

  /** Another test pojo. */
  public static class SubPojo implements Serializable {
    private String description;
    private Double progress;

    public String getDescription() {
      return description;

    }
    public Double getProgress() {
      return progress;
    }

    public SubPojo(String description, Double progress) {
      this.description = description;
      this.progress = progress;
    }
  }

  @Test
  public void testConvertToRow() throws Exception {

    String json1 = OBJECT_MAPPER.writeValueAsString(
        new Pojo(1,
                 "name1",
                 new SubPojo("subpojo 1", 1.2),
                 Arrays.asList(
                     new SubPojo("description 1.1", 3.3),
                     new SubPojo("description 1.2", 4.3)),
                 Arrays.asList("pink", "magenta")));

    Row row1 =
        Row
            .withRowType(POJO_ROW_TYPE)
            .addValues(
                1,
                "name1",
                Arrays.asList("pink", "magenta"),
                Row.withRowType(SUBPOJO_ROW_TYPE).addValues("subpojo 1", 1.2d).build(),
                Arrays.asList(
                    Row.withRowType(SUBPOJO_ROW_TYPE).addValues("description 1.1", 3.3d).build(),
                    Row.withRowType(SUBPOJO_ROW_TYPE).addValues("description 1.2", 4.3d).build()))
            .build();

    String json2 = OBJECT_MAPPER.writeValueAsString(
        new Pojo(2,
                 "name2",
                 new SubPojo("subpojo 2", 321.2),
                 Arrays.asList(
                     new SubPojo("description 2.2", 13.3),
                     new SubPojo("description 2.3", 14.3)),
                 Arrays.asList("cyan", "blue")));

    Row row2 =
        Row
            .withRowType(POJO_ROW_TYPE)
            .addValues(
                2,
                "name2",
                Arrays.asList("cyan", "blue"),
                Row.withRowType(SUBPOJO_ROW_TYPE).addValues("subpojo 2", 321.2d).build(),
                Arrays.asList(
                    Row.withRowType(SUBPOJO_ROW_TYPE).addValues("description 2.2", 13.3d).build(),
                    Row.withRowType(SUBPOJO_ROW_TYPE).addValues("description 2.3", 14.3d).build()))
            .build();

    PCollection<String> input = pipeline.apply(Create.of(json1, json2));

    PCollection<Row> output = input.apply(FromJson.toRow(POJO_ROW_TYPE));

    PAssert
        .that(output)
        .containsInAnyOrder(row1, row2);

    pipeline.run();
  }
}
