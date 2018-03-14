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

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;

/**
 * Generates Rows.
 *
 * <p>See {@link FakeData} for schema.
 */
public class FakeUnboundedSource extends UnboundedSource<Row, NoOpCheckpointMark> {

  public static FakeUnboundedSource rowEvery100ms() {
    return new FakeUnboundedSource();
  }

  private FakeUnboundedSource() {

  }

  @Override
  public List<? extends UnboundedSource<Row, NoOpCheckpointMark>> split(
      int desiredNumSplits,
      PipelineOptions options) throws Exception {

    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<Row> createReader(
      PipelineOptions options,
      @Nullable NoOpCheckpointMark checkpointMark) throws IOException {

    return new FakeUnboundedReader();
  }

  @Override
  public Coder<NoOpCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(NoOpCheckpointMark.class);
  }

  @Override
  public Coder<Row> getOutputCoder() {
    return FakeData.ROW_TYPE.getRowCoder();
  }

  /**
   * Fake unbounded reader.
   */
  public class FakeUnboundedReader extends UnboundedReader<Row> {

    private volatile boolean hasRow = false;
    private volatile Row row;
    private volatile int count;
    private volatile boolean needToAdvance = true;

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      generateRow();
      return true;
    }

    private synchronized void generateRow() {
      delay();
      row = FakeData.newRow(count++, "row with id " + count, new Date());
      needToAdvance = false;
      hasRow = true;
    }

    private void delay() {
      if (row != null) {
        Duration timeSinceLastRow = new Interval(getRowTimestamp(), DateTime.now()).toDuration();

        if (timeSinceLastRow.isShorterThan(Duration.millis(100))) {
          try {
            Thread.sleep(100L - timeSinceLastRow.getMillis());
          } catch (InterruptedException e) {
            throw new RuntimeException("Row generation delay interrupted", e);
          }
        }
      }
    }

    @Override
    public Row getCurrent() throws NoSuchElementException {
      if (needToAdvance) {
        throw new NoSuchElementException("No row available");
      }

      needToAdvance = true;
      return row;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (!hasRow) {
        throw new NoSuchElementException("No row available");
      }

      return getRowTimestamp();
    }

    private Instant getRowTimestamp() {
      return new DateTime(row.<Date> getValue("timestamp")).toInstant();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Instant getWatermark() {
      return getCurrentTimestamp();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return NoOpCheckpointMark.INSTANCE;
    }

    @Override
    public UnboundedSource<Row, ?> getCurrentSource() {
      return FakeUnboundedSource.this;
    }
  }

}
