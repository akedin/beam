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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * BeamSqlConnection.
 */
public class BeamSqlConnection implements Connection {

  private volatile boolean closed;

  public static BeamSqlConnection fakeDataConnection() {
    return new BeamSqlConnection();
  }

  private BeamSqlConnection() {
  }

  /**
   * Basic queries.
   */
  @Override
  public Statement createStatement() {
    return BeamSqlStatement.of(this);
  }

  /**
   * Parametrized queries.
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Prepared statements are not supported yet");
  }

  /**
   * Stored procedures.
   */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Prepare call is not supported yet");
  }

  /**
   * Converts JDBC SQL to native SQL.
   */
  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Native SQL is not supported yet");
  }

  /**
   * Automatically commits each statement.
   */
  @Override
  public void setAutoCommit(boolean autoCommit) { }

  /**
   * Automatically commit each statement.
   */
  @Override
  public boolean getAutoCommit() {
    return true;
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Cannot commit in Beam SQL");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("Cannot rollback in Beam SQL");
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public DatabaseMetaData getMetaData() {
    return BeamSqlDatabaseMetadata.of(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLFeatureNotSupportedException("Cannot set read-only flag on Beam SQL");
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  /**
   * Sets the active catalog.
   */
  @Override
  public void setCatalog(String catalog) {
    // no-op
  }

  @Override
  public String getCatalog() {
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) {
    // no-op
  }

  @Override
  public int getTransactionIsolation() {
    // Transactions are not supported.
    return TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.isClosed()) {
      throw new SQLException("Cannot get warnings from closed connection");
    }

    return null;
  }

  @Override
  public void clearWarnings() { }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
          "Only ResultSet.TYPE_FORWARD_ONLY with ResultSet.CONCUR_READ_ONLY are supported");
    }

    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency)  throws SQLException {

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
          "Only ResultSet.TYPE_FORWARD_ONLY with ResultSet.CONCUR_READ_ONLY are supported");
    }

    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
          "Only ResultSet.TYPE_FORWARD_ONLY with ResultSet.CONCUR_READ_ONLY are supported");
    }

    return prepareCall(sql);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    return Collections.emptyMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Setting custom type map is not supported by Beam SQL");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are unsupported in Beam SQL");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Rollbacks are unsupported in Beam SQL");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are unsupported in Beam SQL");
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {

    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {

    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {

    throw new SQLFeatureNotSupportedException("Holdability is unsupported in Beam SQL");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {

    throw new SQLFeatureNotSupportedException("Autogenerated keys are unsupported in Beam SQL");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {

    throw new SQLFeatureNotSupportedException("Autogenerated keys are unsupported in Beam SQL");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {

      throw new SQLFeatureNotSupportedException("Autogenerated keys are unsupported in Beam SQL");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Creating CLOBs is unsupported in Beam SQL");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Creating BLOBs is unsupported in Beam SQL");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Creating NCLOBs is unsupported in Beam SQL");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("Creating SQLXMLs unsupported in Beam SQL");
  }

  @Override
  public boolean isValid(int timeout) {
    return !this.isClosed();
  }

  @Override
  public void setClientInfo(String name, String value) {
    // no-op for now
  }

  @Override
  public void setClientInfo(Properties properties) {
    // no-op for now
  }

  @Override
  public String getClientInfo(String name) {
    return "Fake Beam SQL client. Nothing more.";
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException("Getting client info is unsupported in Beam SQL");
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Creating arrays is unsupported in Beam SQL");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Getting client info is unsupported in Beam SQL");
  }

  @Override
  public void setSchema(String schema) {
    // no-op
  }

  @Override
  public String getSchema() {
    return "Fake Beam SQL schema";
  }

  @Override
  public void abort(Executor executor) {
    // no-op
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) {
    // no-op
  }

  @Override
  public int getNetworkTimeout() {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Unwrapping is unsupported in Beam SQL");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Wrapping is unsupported in Beam SQL");

  }
}
