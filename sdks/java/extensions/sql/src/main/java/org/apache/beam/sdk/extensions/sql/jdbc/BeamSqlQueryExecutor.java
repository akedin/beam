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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * BeamSqlQueryExecutor.
 */
public class BeamSqlQueryExecutor {

  private ExecutorService executorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("beam-sql-query-executor-thread-%d").build());

  private Statement statement;
  private Future<?> executionFuture;

  public static BeamSqlQueryExecutor of(Statement statement) {
    return new BeamSqlQueryExecutor(statement);
  }

  private BeamSqlQueryExecutor(Statement statement) {
    this.statement = statement;
  }

  public BeamSqlResultSet execute(String sql) {
    if (executionFuture != null && !executionFuture.isCancelled() && !executionFuture.isDone()) {
      throw new IllegalStateException(
          "Query execution should be aborted before starting the new one");
    }

    executionFuture = executorService.submit(
        () ->
            FakePipelineRunner.run(
                sql,
                TheBestRobustInMemoryRowsStorage.getInstance()));

    return
        BeamSqlResultSet.of(
            statement,
            FakeData.ROW_TYPE,
            TheBestRobustInMemoryRowsStorage.getInstance());
  }

  public void abort() {
    if (executionFuture != null && !executionFuture.isCancelled()) {
      executionFuture.cancel(true);
    }
  }
}
