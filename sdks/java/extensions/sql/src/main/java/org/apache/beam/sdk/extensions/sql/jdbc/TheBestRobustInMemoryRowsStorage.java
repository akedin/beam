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

import java.util.concurrent.LinkedBlockingQueue;

/**
 * What class name says.
 */
class TheBestRobustInMemoryRowsStorage<T>
    implements ResultStore.Reader<T>, ResultStore.Writer<T> {

  private static final TheBestRobustInMemoryRowsStorage INSTANCE =
      new TheBestRobustInMemoryRowsStorage();

  private LinkedBlockingQueue<T> results;

  public static <V> TheBestRobustInMemoryRowsStorage<V> getInstance() {
    return INSTANCE;
  }

  private TheBestRobustInMemoryRowsStorage() {
    this.results = new LinkedBlockingQueue<>(1024);
  }

  @Override
  public boolean hasMoreResults() {
    return true;
  }

  @Override
  public void setPageSize(int size) {

  }

  @Override
  public int getPageSize() {
    return 1000;
  }

  @Override
  public T nextResult() {
    try {
      return results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for next row");
    }
  }

  @Override
  public void addResult(T result) {
    this.results.offer(result);
  }

  @Override
  public void close() {

  }
}
