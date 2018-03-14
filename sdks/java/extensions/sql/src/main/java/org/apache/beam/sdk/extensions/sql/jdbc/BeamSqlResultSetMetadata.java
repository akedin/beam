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

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Date;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.values.RowType;

/**
 * BeamSqlResultSetMetadata.
 */
public class BeamSqlResultSetMetadata implements ResultSetMetaData {

  private static final ImmutableMap<SqlTypeCoder, Integer> JDBC_TYPES =
      ImmutableMap.<SqlTypeCoder, Integer> builder()
          .put(SqlTypeCoders.TINYINT, Types.TINYINT)
          .put(SqlTypeCoders.SMALLINT, Types.SMALLINT)
          .put(SqlTypeCoders.INTEGER, Types.INTEGER)
          .put(SqlTypeCoders.BIGINT, Types.BIGINT)
          .put(SqlTypeCoders.DECIMAL, Types.DECIMAL)
          .put(SqlTypeCoders.CHAR, Types.CHAR)
          .put(SqlTypeCoders.VARCHAR, Types.VARCHAR)
          .put(SqlTypeCoders.DATE, Types.DATE)
          .put(SqlTypeCoders.TIME, Types.TIME)
          .put(SqlTypeCoders.TIMESTAMP, Types.TIMESTAMP)
          .put(SqlTypeCoders.BOOLEAN, Types.BOOLEAN)
          .put(SqlTypeCoders.FLOAT, Types.FLOAT)
          .put(SqlTypeCoders.DOUBLE, Types.DOUBLE)
          .build();

  private static final ImmutableMap<SqlTypeCoder, String> TYPE_NAMES =
      ImmutableMap.<SqlTypeCoder, String> builder()
          .put(SqlTypeCoders.TINYINT, "TINYINT")
          .put(SqlTypeCoders.SMALLINT, "SMALLINT")
          .put(SqlTypeCoders.INTEGER, "INTEGER")
          .put(SqlTypeCoders.BIGINT, "BIGINT")
          .put(SqlTypeCoders.DECIMAL, "DECIMAL")
          .put(SqlTypeCoders.CHAR, "CHAR")
          .put(SqlTypeCoders.VARCHAR, "VARCHAR")
          .put(SqlTypeCoders.DATE, "DATE")
          .put(SqlTypeCoders.TIME, "TIME")
          .put(SqlTypeCoders.TIMESTAMP, "TIMESTAMP")
          .put(SqlTypeCoders.BOOLEAN, "BOOLEAN")
          .put(SqlTypeCoders.FLOAT, "FLOAT")
          .put(SqlTypeCoders.DOUBLE, "DOUBLE")
          .build();

  private static final ImmutableMap<SqlTypeCoder, String> JAVA_TYPES =
      ImmutableMap.<SqlTypeCoder, String> builder()
          .put(SqlTypeCoders.TINYINT, Byte.class.getName())
          .put(SqlTypeCoders.SMALLINT, Short.class.getName())
          .put(SqlTypeCoders.INTEGER, Integer.class.getName())
          .put(SqlTypeCoders.BIGINT, Long.class.getName())
          .put(SqlTypeCoders.DECIMAL, BigDecimal.class.getName())
          .put(SqlTypeCoders.CHAR, String.class.getName())
          .put(SqlTypeCoders.VARCHAR, String.class.getName())
          .put(SqlTypeCoders.DATE, Date.class.getName())
          .put(SqlTypeCoders.TIME, Date.class.getName())
          .put(SqlTypeCoders.TIMESTAMP, Date.class.getName())
          .put(SqlTypeCoders.BOOLEAN, Boolean.class.getName())
          .put(SqlTypeCoders.FLOAT, Float.class.getName())
          .put(SqlTypeCoders.DOUBLE, Double.class.getName())
          .build();

  private BeamSqlResultSet resultSet;
  private RowType schema;

  public static ResultSetMetaData of(
      BeamSqlResultSet beamSqlResultSet,
      RowType schema) {

    return new BeamSqlResultSetMetadata(beamSqlResultSet, schema);
  }

  private BeamSqlResultSetMetadata(
      BeamSqlResultSet resultSet,
      RowType schema) {

    this.resultSet = resultSet;
    this.schema = schema;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return schema.getFieldCount();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return true;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 5;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return schema.getFieldName(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return schema.getFieldName(column - 1);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return resultSet.getStatement().getConnection().getSchema();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return "Result";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "Beam SQL";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    SqlTypeCoder sqlTypeCoder = (SqlTypeCoder) schema.getFieldCoder(column - 1);
    if (JDBC_TYPES.containsKey(sqlTypeCoder)) {
      return JDBC_TYPES.get(sqlTypeCoder);
    }

    throw new SQLFeatureNotSupportedException("Support Beam SQL type "
                                              + sqlTypeCoder.getClass().getSimpleName()
                                              + " is not implemented yet");
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    SqlTypeCoder sqlTypeCoder = (SqlTypeCoder) schema.getFieldCoder(column - 1);
    if (TYPE_NAMES.containsKey(sqlTypeCoder)) {
      return TYPE_NAMES.get(sqlTypeCoder);
    }

    throw new SQLFeatureNotSupportedException("Support Beam SQL type "
                                              + sqlTypeCoder.getClass().getSimpleName()
                                              + " is not implemented yet");
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {

    SqlTypeCoder sqlTypeCoder = (SqlTypeCoder) schema.getFieldCoder(column - 1);
    if (JAVA_TYPES.containsKey(sqlTypeCoder)) {
      return JAVA_TYPES.get(sqlTypeCoder);
    }

    throw new SQLFeatureNotSupportedException("Support Beam SQL type "
                                              + sqlTypeCoder.getClass().getSimpleName()
                                              + " is not implemented yet");  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "Unwrapping of the metadata is not supported in Beam SQL");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
