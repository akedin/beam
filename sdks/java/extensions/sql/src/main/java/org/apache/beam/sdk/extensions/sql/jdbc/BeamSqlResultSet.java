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

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

/**
 * BeamSqlResultSet.
 */
public class BeamSqlResultSet implements ResultSet {

  private ResultStore.Reader<Row> resultsStore;
  private RowType schema;
  private Row currentResult;

  private boolean isBeforeFirst;
  private boolean isFirst;
  private boolean isAfterLast;
  private boolean isClosed;
  private boolean wasNull;
  private Statement statement;

  public static BeamSqlResultSet of(
      Statement statement,
      RowType schema,
      ResultStore.Reader<Row> resultsStore) {
    return new BeamSqlResultSet(statement, schema, resultsStore);
  }

  public static BeamSqlResultSet empty() {
    return of(null, RowSqlType.builder().build(), EmptyResultStoreReader.of());
  }

  private BeamSqlResultSet(
      Statement statement,
      RowType schema,
      ResultStore.Reader<Row> resultsStore) {

    this.statement = statement;
    this.schema = schema;
    this.resultsStore = resultsStore;
    this.isBeforeFirst = true;
    this.isFirst = false;
    this.isAfterLast = false;
    this.isClosed = true;
    this.wasNull = false;
  }

  @Override
  public boolean next() {
    this.isFirst = this.isBeforeFirst;
    this.isBeforeFirst = false;

    if (resultsStore.hasMoreResults()) {
      currentResult = resultsStore.nextResult();
      return true;
    }

    this.isAfterLast = true;
    return false;
  }

  @Override
  public void close() {
    this.isClosed = true;
    resultsStore.close();
  }

  @Override
  public boolean wasNull() {
    return this.wasNull;
  }

  public <V> V getValue(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > currentResult.getFieldCount()) {
      throw new SQLException("Invalid column index "
                             + columnIndex
                             + ". Should be between 1 and "
                             + currentResult.getFieldCount()
                             + "(both inclusive)");
    }

    V value = currentResult.getValue(columnIndex - 1);
    this.wasNull = value == null;
    return value;
  }

  public <V> V getValue(String columnLabel) throws SQLException {
    try {
      V value = currentResult.getValue(columnLabel);

      this.wasNull = value == null;
      return value;
    } catch (IndexOutOfBoundsException e) {
      throw new SQLException("Invalid column label '" + columnLabel + "'", e);
    }
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getValue(columnIndex).toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public SQLWarning getWarnings() {
    // no warnings
    return null;
  }

  @Override
  public void clearWarnings() { }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Cursors are not supported by Beam SQL");
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return BeamSqlResultSetMetadata.of(this, this.schema);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    int index = currentResult.getRowType().indexOf(columnLabel);

    if (index < 0) {
      throw new SQLException("Column '" + columnLabel + "' does not exist");
    }

    return index + 1;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return new StringReader(getString(columnIndex));
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return new StringReader(getString(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public boolean isBeforeFirst() {
    return this.isBeforeFirst;
  }

  @Override
  public boolean isAfterLast() {
    return this.isAfterLast;
  }

  @Override
  public boolean isFirst() {
    return this.isFirst;
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Beam SQL does now support isLast() check on the result set");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Row numbers are not supported in Beam SQL");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException("Moving cursors are not supported in Beam SQL");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Setting fetch direction is not supported in Beam SQL");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Setting fetch direction is not supported in Beam SQL");
  }

  @Override
  public void setFetchSize(int rows) {
    resultsStore.setPageSize(rows);
  }

  @Override
  public int getFetchSize() {
    return resultsStore.getPageSize();
  }

  @Override
  public int getType() {
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() {
    return CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows insertion is not supported in Beam SQL");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows deletion is not supported in Beam SQL");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws
      SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws
      SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows insertion is not supported in Beam SQL");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows deletion is not supported in Beam SQL");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows refreshing is not supported in Beam SQL");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Moving cursors is not supported in Beam SQL");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Moving cursors is not supported in Beam SQL");
  }

  @Override
  public Statement getStatement() {
    return this.statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Beam SQL does not support custom mapping of row fields");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Beam SQL does not support custom mapping of row fields");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  /**
   * Doesn't handle calendar.
   */
  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return getValue(columnIndex);
  }

  /**
   * Doesn't handle calendar.
   */
  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Row IDs are not supported in Beam SQL");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Row IDs are not supported in Beam SQL");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Holdability is not supported by Beam SQL");
  }

  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return new StringReader(getString(columnIndex));
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return new StringReader(getString(columnLabel));
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws
      SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws
      SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws
      SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Rows modification is not supported in Beam SQL");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return (T) this;
    }

    throw new SQLFeatureNotSupportedException(
        this.getClass().getSimpleName()
        + " does not implement " + iface.getSimpleName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(this.getClass());
  }
}
