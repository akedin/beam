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
package org.apache.beam.sdk.extensions.sql.jdbc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * FakePipelineRunner.
 */
public class FakePipelineRunner {

  public static void run(String sql, ResultStore.Writer<Row> rowStorage) {
//    PipelineOptionsFactory.create().setRunner(DirectRunner.class);
//
//    DirectOptions options =
//        PipelineOptionsFactory.as(org.apache.beam.runners.direct.DirectOptions.class);
//    options.setBlockOnRun(false);
//    options.setRunner(PipelineOptions.DirectRunner.class);

    try {
      Pipeline p = Pipeline.create();

      p.apply(Read.from(FakeUnboundedSource.rowEvery100ms()))
       .setCoder(FakeData.ROW_TYPE.getRowCoder())
       .apply(BeamSql.query(sql))
       .apply(ParDo.of(new WriteToStorage()));

      p.run().waitUntilFinish();
    } catch (Throwable e) {
      System.out.println("Error when running the pipeline: " + e);
      throw new RuntimeException("Error when running the pipeline: " + e);
    }
  }


  public static class WriteToStorage extends DoFn<Row, String> {

    static ResultStore.Writer<Row> rowStorage = TheBestRobustInMemoryRowsStorage.getInstance();

    @ProcessElement
    public void processElement(ProcessContext c) {
      rowStorage.addResult(c.element());
      c.output(c.element().getString("description"));
    }
  }
}
