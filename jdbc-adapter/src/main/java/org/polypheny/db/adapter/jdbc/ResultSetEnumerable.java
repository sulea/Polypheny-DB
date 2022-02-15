/*
 * Copyright 2019-2021 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.jdbc;


import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.jdbc.JdbcTypeDefinition.PolyphenyToJdbcMapping;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.sql.SqlDialect.IntervalParameterStrategy;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolySerializer;
import org.polypheny.db.util.Static;


/**
 * Executes a SQL statement and returns the result as an {@link Enumerable}.
 *
 * @param <T> Element type
 */
@Slf4j
public class ResultSetEnumerable<T> extends AbstractEnumerable<T> {

    private final ConnectionHandler connectionHandler;
    private final String sql;
    private final Function1<ResultSet, Function0<T>> rowBuilderFactory;
    private final PreparedStatementEnricher preparedStatementEnricher;

    private Long queryStart;
    private long timeout;
    private boolean timeoutSetFailed;

    private static final Function1<ResultSet, Function0<Object>> AUTO_ROW_BUILDER_FACTORY =
            resultSet -> {
                final ResultSetMetaData metaData;
                final int columnCount;
                try {
                    metaData = resultSet.getMetaData();
                    columnCount = metaData.getColumnCount();
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
                if ( columnCount == 1 ) {
                    return () -> {
                        try {
                            return resultSet.getObject( 1 );
                        } catch ( SQLException e ) {
                            throw new RuntimeException( e );
                        }
                    };
                } else {
                    //noinspection unchecked
                    return (Function0) () -> {
                        try {
                            final List<Object> list = new ArrayList<>();
                            for ( int i = 0; i < columnCount; i++ ) {
                                if ( metaData.getColumnType( i + 1 ) == Types.TIMESTAMP ) {
                                    long v = resultSet.getLong( i + 1 );
                                    if ( v == 0 && resultSet.wasNull() ) {
                                        list.add( null );
                                    } else {
                                        list.add( v );
                                    }
                                } else {
                                    list.add( resultSet.getObject( i + 1 ) );
                                }
                            }
                            return list.toArray();
                        } catch ( SQLException e ) {
                            throw new RuntimeException( e );
                        }
                    };
                }
            };


    private ResultSetEnumerable(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<T>> rowBuilderFactory,
            PreparedStatementEnricher preparedStatementEnricher ) {
        this.connectionHandler = connectionHandler;
        this.sql = sql;
        this.rowBuilderFactory = rowBuilderFactory;
        this.preparedStatementEnricher = preparedStatementEnricher;
    }


    private ResultSetEnumerable(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<T>> rowBuilderFactory ) {
        this( connectionHandler, sql, rowBuilderFactory, null );
    }


    /**
     * Creates an ResultSetEnumerable.
     */
    public static ResultSetEnumerable<Object> of(
            ConnectionHandler connectionHandler,
            String sql ) {
        return of( connectionHandler, sql, AUTO_ROW_BUILDER_FACTORY );
    }


    /**
     * Creates an ResultSetEnumerable that retrieves columns as specific Java types.
     */
    public static ResultSetEnumerable<Object> of(
            ConnectionHandler connectionHandler,
            String sql,
            Primitive[] primitives ) {
        return of( connectionHandler, sql, primitiveRowBuilderFactory( primitives ) );
    }


    /**
     * Executes a SQL query and returns the results as an enumerator, using a row builder to convert
     * JDBC column values into rows.
     */
    public static <T> ResultSetEnumerable<T> of(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<T>> rowBuilderFactory ) {
        return new ResultSetEnumerable<>( connectionHandler, sql, rowBuilderFactory );
    }


    /**
     * Executes a SQL query and returns the results as an enumerator, using a row builder to convert
     * JDBC column values into rows.
     *
     * It uses a {@link PreparedStatement} for computing the query result, and that means that it can bind parameters.
     */
    public static <T> ResultSetEnumerable<T> of(
            ConnectionHandler connectionHandler,
            String sql,
            Function1<ResultSet, Function0<T>> rowBuilderFactory,
            PreparedStatementEnricher consumer ) {
        return new ResultSetEnumerable<>( connectionHandler, sql, rowBuilderFactory, consumer );
    }


    public void setTimeout( DataContext context ) {
        this.queryStart = (Long) context.get( DataContext.Variable.UTC_TIMESTAMP.camelName );
        Object timeout = context.get( DataContext.Variable.TIMEOUT.camelName );
        if ( timeout instanceof Long ) {
            this.timeout = (Long) timeout;
        } else {
            if ( timeout != null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Variable.TIMEOUT should be `long`. Given value was {}", timeout );
                }
            }
            this.timeout = 0;
        }
    }


    /**
     * Called from generated code that proposes to create a {@code ResultSetEnumerable} over a prepared statement.
     */
    public static PreparedStatementEnricher createEnricher( Integer[] indexes, DataContext context ) {
        return ( preparedStatement, connectionHandler ) -> {

            boolean batch = false;
            if ( context.getParameterValues().size() > 1 ) {
                batch = true;
            }
            for ( Map<Long, Object> values : context.getParameterValues() ) {
                for ( int i = 0; i < indexes.length; i++ ) {
                    final long index = indexes[i];
                    setDynamicParam(
                            preparedStatement,
                            i + 1,
                            values.get( index ),
                            context.getParameterType( index ),
                            preparedStatement.getParameterMetaData().getParameterType( i + 1 ),
                            connectionHandler );
                }
                if ( batch ) {
                    preparedStatement.addBatch();
                }
            }
            return batch;
        };
    }


    /**
     * Assigns a value to a dynamic parameter in a prepared statement, calling the appropriate {@code setXxx}
     * method based on the type of the parameter.
     */
    private static void setDynamicParam( PreparedStatement preparedStatement, int i, Object value, AlgDataType type, int sqlType, ConnectionHandler connectionHandler ) throws SQLException {
        if ( value == null ) {
            preparedStatement.setNull( i, SqlType.NULL.id );
        } else {
            Object mapped = PolyphenyToJdbcMapping.INSTANCE.map( value, type );

            switch ( type.getPolyType() ) {
                case NULL:
                    preparedStatement.setNull( i, SqlType.NULL.id );
                    break;
                case BOOLEAN:
                    preparedStatement.setBoolean( i, PolyphenyToJdbcMapping.INSTANCE.toBoolean( value ) );
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    preparedStatement.setInt( i, (Integer) mapped );
                    break;
                case BIGINT:
                    preparedStatement.setLong( i, (Long) mapped );
                    break;
                case DECIMAL:
                    preparedStatement.setBigDecimal( i, (BigDecimal) mapped );
                    break;
                case FLOAT:
                case REAL:
                    preparedStatement.setFloat( i, (Float) mapped );
                    break;
                case DOUBLE:
                    preparedStatement.setDouble( i, (Double) mapped );
                    break;
                case DATE:
                    preparedStatement.setDate( i, (java.sql.Date) mapped );
                    break;
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                    preparedStatement.setTime( i, (java.sql.Time) mapped );
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    preparedStatement.setTimestamp( i, (java.sql.Timestamp) mapped );
                    break;
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    if ( connectionHandler.getDialect().getIntervalParameterStrategy() == IntervalParameterStrategy.MULTIPLICATION ) {
                        preparedStatement.setInt( i, ((BigDecimal) mapped).intValue() );
                    } else if ( connectionHandler.getDialect().getIntervalParameterStrategy() == IntervalParameterStrategy.CAST ) {
                        preparedStatement.setString( i, mapped + " " + type.getIntervalQualifier().getTimeUnitRange().name() );
                    } else {
                        throw new RuntimeException( "Unknown IntervalParameterStrategy: " + connectionHandler.getDialect().getIntervalParameterStrategy().name() );
                    }
                    break;
                case BINARY:
                case VARBINARY:
                    preparedStatement.setBytes( i, (byte[]) mapped );
                    break;

                case CHAR:
                case VARCHAR:
                case ANY:
                case SYMBOL:
                case JSON:
                    preparedStatement.setString( i, (String) mapped );
                    break;
                case MULTISET:
                case ARRAY:
                case ROW:
                    if ( connectionHandler.getDialect().supportsNestedArrays() ) {
                        SqlType componentType;
                        AlgDataType t = type;
                        while ( t.getComponentType().getPolyType() == PolyType.ARRAY ) {
                            t = t.getComponentType();
                        }
                        componentType = SqlType.valueOf( t.getComponentType().getPolyType().getJdbcOrdinal() );
                        Array array = connectionHandler.createArrayOf( connectionHandler.getDialect().getArrayComponentTypeString( componentType ), ((List<?>) mapped).toArray() );
                        preparedStatement.setArray( i, array );
                    } else {
                        preparedStatement.setString( i, PolySerializer.serializeArray( mapped ) );
                    }
                    break;
                case FILE:
                case IMAGE:
                case VIDEO:
                case SOUND:
                    if ( connectionHandler.getDialect().supportsBinaryStream() ) {
                        preparedStatement.setBinaryStream( i, new ByteArrayInputStream( (byte[]) mapped ) );
                    } else {
                        preparedStatement.setBytes( i, (byte[]) mapped );
                    }
                    break;
                case CURSOR:
                case COLUMN_LIST:
                case DYNAMIC_STAR:
                case GEOMETRY:
                case MAP:
                case DISTINCT:
                case STRUCTURED:
                case OTHER:
                    throw new UnsupportedOperationException( "Type not yet considered." );
            }
        }
    }


    @Override
    public Enumerator<T> enumerator() {
        if ( preparedStatementEnricher == null ) {
            return enumeratorBasedOnStatement();
        } else {
            return enumeratorBasedOnPreparedStatement();
        }
    }


    /*private Enumerator<T> enumeratorBasedOnStatement() {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = connectionHandler.getConnection();
            statement = connection.createStatement();
            setTimeoutIfPossible( statement );
            if ( statement.execute( sql ) ) {
                final ResultSet resultSet = statement.getResultSet();
                statement = null;
                connection = null;
                return new ResultSetEnumerator<>( resultSet, rowBuilderFactory );
            } else {
                Integer updateCount = statement.getUpdateCount();
                return Linq4j.singletonEnumerator( (T) updateCount );
            }
        } catch ( SQLException e ) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema( sql ).ex( e );
        } finally {
            closeIfPossible( connection, statement );
        }
    }*/


    private Enumerator<T> enumeratorBasedOnStatement() {
        Statement statement = null;
        try {
            statement = connectionHandler.getStatement();
            setTimeoutIfPossible( statement );
            if ( statement.execute( sql ) ) {
                final ResultSet resultSet = statement.getResultSet();
                statement = null;
                return new ResultSetEnumerator<>( resultSet, rowBuilderFactory );
            } else {
                Integer updateCount = statement.getUpdateCount();
                return Linq4j.singletonEnumerator( (T) updateCount );
            }
        } catch ( SQLException e ) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema( sql ).ex( e );
        } finally {
            closeIfPossible( statement );
        }
    }


    private Enumerator<T> enumeratorBasedOnPreparedStatement() {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connectionHandler.prepareStatement( sql );
            setTimeoutIfPossible( preparedStatement );
            if ( preparedStatementEnricher.enrich( preparedStatement, connectionHandler ) ) {
                // batch
                preparedStatement.executeBatch();
                Integer updateCount = preparedStatement.getUpdateCount();
                return Linq4j.singletonEnumerator( (T) updateCount );
            } else {
                if ( preparedStatement.execute() ) {
                    final ResultSet resultSet = preparedStatement.getResultSet();
                    preparedStatement = null;
                    return new ResultSetEnumerator<>( resultSet, rowBuilderFactory );
                } else {
                    Integer updateCount = preparedStatement.getUpdateCount();
                    return Linq4j.singletonEnumerator( (T) updateCount );
                }
            }
        } catch ( SQLException e ) {
            throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema( sql ).ex( e );
        } finally {
            closeIfPossible( preparedStatement );
        }
    }


    private void setTimeoutIfPossible( Statement statement ) throws SQLException {
        if ( timeout == 0 ) {
            return;
        }
        long now = System.currentTimeMillis();
        long secondsLeft = (queryStart + timeout - now) / 1000;
        if ( secondsLeft <= 0 ) {
            throw Static.RESOURCE.queryExecutionTimeoutReached( String.valueOf( timeout ), String.valueOf( Instant.ofEpochMilli( queryStart ) ) ).ex();
        }
        if ( secondsLeft > Integer.MAX_VALUE ) {
            // Just ignore the timeout if it happens to be too big, we can't squeeze it into int
            return;
        }
        try {
            statement.setQueryTimeout( (int) secondsLeft );
        } catch ( SQLFeatureNotSupportedException e ) {
            if ( !timeoutSetFailed && log.isDebugEnabled() ) {
                // We don't really want to print this again and again if enumerable is used multiple times
                log.debug( "Failed to set query timeout {} seconds", secondsLeft, e );
                timeoutSetFailed = true;
            }
        }
    }


    private void closeIfPossible( Statement statement ) {
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }


    /**
     * Implementation of {@link Enumerator} that reads from a {@link ResultSet}.
     *
     * @param <T> element type
     */
    private static class ResultSetEnumerator<T> implements Enumerator<T> {

        private final Function0<T> rowBuilder;
        private ResultSet resultSet;


        ResultSetEnumerator( ResultSet resultSet, Function1<ResultSet, Function0<T>> rowBuilderFactory ) {
            this.resultSet = resultSet;
            this.rowBuilder = rowBuilderFactory.apply( resultSet );
        }


        @Override
        public T current() {
            return rowBuilder.apply();
        }


        @Override
        public boolean moveNext() {
            try {
                return resultSet.next();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }


        @Override
        public void reset() {
            try {
                resultSet.beforeFirst();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }


        @Override
        public void close() {
            ResultSet savedResultSet = resultSet;
            if ( savedResultSet != null ) {
                try {
                    resultSet = null;
                    final Statement statement = savedResultSet.getStatement();
                    savedResultSet.close();
                    if ( statement != null ) {
                        //final Connection connection = statement.getConnection();
                        statement.close();
                        /*if ( connection != null ) {
                            connection.close();
                        }*/
                    }
                } catch ( SQLException e ) {
                    // ignore
                }
            }
        }

    }


    private static Function1<ResultSet, Function0<Object>>
    primitiveRowBuilderFactory( final Primitive[] primitives ) {
        return resultSet -> {
            final ResultSetMetaData metaData;
            final int columnCount;
            try {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
            assert columnCount == primitives.length;
            if ( columnCount == 1 ) {
                return () -> {
                    try {
                        return resultSet.getObject( 1 );
                    } catch ( SQLException e ) {
                        throw new RuntimeException( e );
                    }
                };
            }
            //noinspection unchecked
            return (Function0) () -> {
                try {
                    final List<Object> list = new ArrayList<>();
                    for ( int i = 0; i < columnCount; i++ ) {
                        list.add( primitives[i].jdbcGet( resultSet, i + 1 ) );
                    }
                    return list.toArray();
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
            };
        };
    }


    /**
     * Consumer for decorating a {@link PreparedStatement}, that is, setting its parameters.
     */
    public interface PreparedStatementEnricher {

        // returns true if this needs to be executed as batch
        boolean enrich( PreparedStatement statement, ConnectionHandler connectionHandler ) throws SQLException;

    }

}

