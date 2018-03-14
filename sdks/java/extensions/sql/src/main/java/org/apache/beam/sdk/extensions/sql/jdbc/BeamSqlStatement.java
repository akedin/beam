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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * BeamSqlStatement.
 */
public class BeamSqlStatement implements Statement {

  private BeamSqlConnection connection;
  private BeamSqlQueryExecutor queryExecutor;
  private BeamSqlResultSet resultSet;
  private boolean isClosed;

  public static Statement of(BeamSqlConnection beamSqlConnection) {
    return new BeamSqlStatement(beamSqlConnection);
  }

  private BeamSqlStatement(BeamSqlConnection beamSqlConnection) {
    connection = beamSqlConnection;
    queryExecutor = BeamSqlQueryExecutor.of(this);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    queryExecutor.abort();
    this.resultSet = queryExecutor.execute(sql);
    return getResultSet();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Update queries are not supported in Beam SQL");
  }

  @Override
  public void close() throws SQLException {
    this.isClosed = true;
    cancel();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Max field size is not supported in Beam SQL");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Max field size is not supported in Beam SQL");
  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Setting max rows is not supported in Beam SQL");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Escape processing is not supported in Beam SQL");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("Query timeout is not supported in Beam SQL");
  }

  @Override
  public void cancel() throws SQLException {
    queryExecutor.abort();
    resultSet.close();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Cursors are not supported in Beam SQL");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    queryExecutor.abort();
    this.resultSet = queryExecutor.execute(sql);
    return true;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Setting fetch direction is not supported in Beam SQL");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    this.resultSet.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return this.resultSet.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch query execution is not supported in Beam SQL");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch query execution is not supported in Beam SQL");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch query execution is not supported in Beam SQL");
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("Generated keys are not supported in Beam SQL");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Updates are not supported in Beam SQL");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Updates are not supported in Beam SQL");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Updates are not supported in Beam SQL");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Generated keys are not supported in Beam SQL");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Column indexes are not supported in Beam SQL");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Column indexes are not supported in Beam SQL");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Holdability is not supported in Beam SQL");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Pooling is not supported in Beam SQL");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException("Closing on completion is not supported in Beam SQL");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Unwrapping of the statement is not supported in Beam SQL");  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

}
