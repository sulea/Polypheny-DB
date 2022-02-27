/*
 * Copyright 2019-2022 The Polypheny Project
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
 */

package org.polypheny.db.adapter.jdbc;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeMapping;
import org.polypheny.db.type.mapping.PolyphenyTypeDefinition;
import org.polypheny.db.type.mapping.TypeDefinition;
import org.polypheny.db.type.mapping.TypeSpaceMapping;
import org.polypheny.db.type.mapping.TypeSpaceMapping.UnsupportedTypeException;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.PolySerializer;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public enum JdbcTypeDefinition implements TypeDefinition<JdbcTypeDefinition> {
    INSTANCE;


    @Override
    public TypeSpaceMapping<JdbcTypeDefinition, PolyphenyTypeDefinition> getToPolyphenyMapping() {
        return JdbcToPolyphenyMapping.INSTANCE;
    }


    @Override
    public TypeSpaceMapping<PolyphenyTypeDefinition, JdbcTypeDefinition> getFromPolyphenyMapping() {
        return PolyphenyToJdbcMapping.INSTANCE;
    }


    @Override
    public List<Class<?>> getMappingClasses( PolyType type ) {
        switch ( type ) {

            case BOOLEAN:
                return Arrays.asList( Boolean.class, boolean.class );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Arrays.asList( Integer.class, int.class );
            case BIGINT:
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
                return Arrays.asList( Long.class, long.class );
            case DECIMAL:
                return Collections.singletonList( BigDecimal.class );
            case FLOAT:
            case REAL:
                return Arrays.asList( Float.class, float.class );
            case DOUBLE:
                return Arrays.asList( Double.class, double.class );
            case DATE:
                return Collections.singletonList( Date.class );
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIME:
                return Collections.singletonList( Time.class );
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Collections.singletonList( Timestamp.class );
            case CHAR:
            case VARCHAR:
            case JSON:
                return Collections.singletonList( String.class );
            case BINARY:
            case VARBINARY:
                return Collections.singletonList( Byte[].class );
            case NULL:
            case GEOMETRY:
            case DYNAMIC_STAR:
            case CURSOR:
            case OTHER:
            case STRUCTURED:
            case DISTINCT:
            case SYMBOL:
            case ANY:
                return Collections.singletonList( Object.class );
            case MULTISET:
            case COLUMN_LIST:
            case ROW:
            case ARRAY:
                return Collections.singletonList( List.class );
            case MAP:
                return Collections.singletonList( Map.class );
            case FILE:
            case SOUND:
            case VIDEO:
            case IMAGE:
                return Arrays.asList( ByteString.class, InputStream.class );
        }

        throw new UnsupportedTypeException( "Jdbc", type );
    }


    public enum JdbcToPolyphenyMapping implements TypeSpaceMapping<JdbcTypeDefinition, PolyphenyTypeDefinition> {
        INSTANCE;

        @Getter
        final Class<JdbcTypeDefinition> from = JdbcTypeDefinition.class;
        @Getter
        final Class<PolyphenyTypeDefinition> to = PolyphenyTypeDefinition.class;


        @Override
        public NlsString toJson( Object obj ) {
            return new NlsString( (String) obj, StandardCharsets.UTF_8.name(), Collation.IMPLICIT );
        }


        @Override
        public ByteString toMultimedia( Object obj ) {
            return new ByteString( (byte[]) obj );
        }


        @Override
        public Object toObject( Object obj ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
            return PolySerializer.deserializeMap( (String) obj );
        }


        @Override
        public PolyList<?> toArray( Object obj, PolyType componentType ) {
            PolyList<Comparable<?>> list = new PolyList<>();
            ((List<?>) obj).forEach( e -> list.add( (Comparable<?>) map( e, componentType, null, null ) ) );
            return list;
        }


        @Override
        public Enum<?> toSymbol( Object obj ) {
            return (Enum<?>) obj;
        }


        @Override
        public Object toAny( Object obj ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Object toNull( Object obj ) {
            return obj;
        }


        @Override
        public ByteString toBinary( Object obj ) {
            return new ByteString( (byte[]) obj );
        }


        @Override
        public NlsString toVarchar( Object obj ) {
            return TypeDefinition.getNlsString( (String) obj );
        }


        @Override
        public NlsString toChar( Object obj ) {
            return toVarchar( obj );
        }


        @Override
        public BigDecimal toInterval( Object obj ) {
            return (BigDecimal) obj;
        }


        @Override
        public TimestampString toTimestamp( Object obj ) {
            return TimestampString.fromMillisSinceEpoch( ((java.sql.Timestamp) obj).getTime() );
        }


        @Override
        public TimeString toTime( Object obj ) {
            return new TimeString( obj.toString() );
        }


        @Override
        public DateString toDate( Object obj ) {
            return new DateString( obj.toString() );
        }


        @Override
        public BigDecimal toDouble( Object obj ) {
            return BigDecimal.valueOf( (Double) obj );
        }


        @Override
        public BigDecimal toReal( Object obj ) {
            return toFloat( obj );
        }


        @Override
        public BigDecimal toFloat( Object obj ) {
            return BigDecimal.valueOf( (Float) obj );
        }


        @Override
        public BigDecimal toDecimal( Object obj ) {
            return (BigDecimal) obj;
        }


        @Override
        public Long toBigInt( Object obj ) {
            if ( obj instanceof Integer ) {
                return Long.valueOf( (Integer) obj );
            }
            return (Long) obj;
        }


        @Override
        public Integer toInteger( Object obj ) {
            return (Integer) obj;
        }


        @Override
        public Integer toSmallInt( Object obj ) {
            return Integer.valueOf( ((Short) obj) );
        }


        @Override
        public Integer toTinyInt( Object obj ) {
            if ( obj instanceof Byte ) {
                return Integer.valueOf( (Byte) obj );
            } else {
                return (Integer) obj;
            }
        }


        @Override
        public Boolean toBoolean( Object obj ) {
            return (Boolean) obj;
        }
    }


    public enum PolyphenyToJdbcMapping implements TypeSpaceMapping<PolyphenyTypeDefinition, JdbcTypeDefinition> {
        INSTANCE;

        @Getter
        final Class<PolyphenyTypeDefinition> from = PolyphenyTypeDefinition.class;
        @Getter
        final Class<JdbcTypeDefinition> to = JdbcTypeDefinition.class;


        @Override
        public String toJson( Object obj ) {
            return ((NlsString) obj).getValue();
        }


        @Override
        public byte[] toMultimedia( Object obj ) {
            return ((ByteString) obj).getBytes();
        }


        @Override
        public Object toObject( Object obj ) {
            return obj;
        }


        @Override
        public String toMap( Object obj, PolyType keyType, PolyType valueType ) {
            return PolySerializer.serializeMap( (PolyMap<?, ?>) obj );
        }


        @Override
        public PolyList<?> toArray( Object obj, PolyType componentType ) {
            PolyList<Comparable<?>> list = new PolyList<>();
            ((PolyList<?>) obj).forEach( e -> list.add( (Comparable<?>) map( e, componentType, null, null ) ) );
            return list;
        }


        @Override
        public Object toSymbol( Object obj ) {
            return obj;
        }


        @Override
        public Object toAny( Object obj ) {
            return obj;
        }


        @Override
        public Object toNull( Object obj ) {
            return obj;
        }


        @Override
        public byte[] toBinary( Object obj ) {
            return ((ByteString) obj).getBytes();
        }


        @Override
        public String toVarchar( Object obj ) {
            return ((NlsString) obj).getValue();
        }


        @Override
        public String toChar( Object obj ) {
            return toVarchar( obj );
        }


        @Override
        public Object toInterval( Object obj ) {
            return obj;
        }


        @Override
        public java.sql.Timestamp toTimestamp( Object obj ) {
            return new java.sql.Timestamp( ((TimestampString) obj).getMillisSinceEpoch() - PolyTypeMapping.TIMEZONE_OFFSET );
        }


        @Override
        public java.sql.Time toTime( Object obj ) {
            return new java.sql.Time( ((TimeString) obj).getMillisOfDay() - PolyTypeMapping.TIMEZONE_OFFSET );
        }


        @Override
        public java.sql.Date toDate( Object obj ) {
            return new java.sql.Date( ((DateString) obj).getMillisSinceEpoch() );
        }


        @Override
        public Double toDouble( Object obj ) {
            return ((BigDecimal) obj).doubleValue();
        }


        @Override
        public Float toReal( Object obj ) {
            return toFloat( obj );
        }


        @Override
        public Float toFloat( Object obj ) {
            return ((BigDecimal) obj).floatValue();
        }


        @Override
        public BigDecimal toDecimal( Object obj ) {
            return (BigDecimal) obj;
        }


        @Override
        public Long toBigInt( Object obj ) {
            return (Long) obj;
        }


        @Override
        public Integer toInteger( Object obj ) {
            return (Integer) obj;
        }


        @Override
        public Integer toSmallInt( Object obj ) {
            return toInteger( obj );
        }


        @Override
        public Integer toTinyInt( Object obj ) {
            return toInteger( obj );
        }


        @Override
        public Boolean toBoolean( Object obj ) {
            return (Boolean) obj;
        }

    }


}
