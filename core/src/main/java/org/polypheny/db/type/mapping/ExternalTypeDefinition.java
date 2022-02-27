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

package org.polypheny.db.type.mapping;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.adapter.java.Array;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.mapping.TypeSpaceMapping.UnsupportedTypeException;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public enum ExternalTypeDefinition implements TypeDefinition<ExternalTypeDefinition> {
    INSTANCE;


    @Override
    public TypeSpaceMapping<ExternalTypeDefinition, PolyphenyTypeDefinition> getToPolyphenyMapping() {
        return ExternalToPolyphenyMapping.INSTANCE;
    }


    @Override
    public TypeSpaceMapping<PolyphenyTypeDefinition, ExternalTypeDefinition> getFromPolyphenyMapping() {
        return PolyphenyToExternalMapping.INSTANCE;
    }


    @Override
    public List<Class<?>> getMappingClasses( PolyType type ) {
        switch ( type ) {

            case BOOLEAN:
                return Arrays.asList( Boolean.class, boolean.class );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
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
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return Arrays.asList( Long.class, long.class );
            case DECIMAL:
                return Collections.singletonList( BigDecimal.class );
            case FLOAT:
            case REAL:
                return Arrays.asList( Float.class, float.class );
            case DOUBLE:
                return Arrays.asList( Double.class, double.class );
            case CHAR:
            case VARCHAR:
            case JSON:
                return Collections.singletonList( String.class );
            case BINARY:
            case VARBINARY:
            case SOUND:
            case VIDEO:
            case IMAGE:
            case FILE:
                return Arrays.asList( Byte[].class, byte[].class );
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
            case ARRAY:
            case MULTISET:
            case ROW:
                return Collections.singletonList( List.class );
            case MAP:
                return Collections.singletonList( Map.class );
            case COLUMN_LIST:
                return Collections.singletonList( Array.class );
        }

        throw new UnsupportedTypeException( "External", type );
    }


    @Slf4j
    public enum PolyphenyToExternalMapping implements TypeSpaceMapping<PolyphenyTypeDefinition, ExternalTypeDefinition> {
        INSTANCE;

        @Getter
        final Class<PolyphenyTypeDefinition> from = PolyphenyTypeDefinition.class;
        @Getter
        final Class<ExternalTypeDefinition> to = ExternalTypeDefinition.class;


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
        public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
            PolyMap<Comparable<?>, Comparable<?>> map = new PolyMap<>();
            ((PolyMap<?, ?>) obj).forEach( ( k, v ) -> map.put( (Comparable<?>) map( k, keyType, null, null ), (Comparable<?>) map( v, valueType, null, null ) ) );

            return ((PolyMap<?, ?>) obj);
        }


        @Override
        public PolyList<?> toArray( Object obj, PolyType innerType ) {
            PolyList<Comparable<?>> list = new PolyList<>();
            ((PolyList<?>) obj).forEach( ( e ) -> list.add( (Comparable<?>) map( e, innerType, null, null ) ) );
            return list;
        }


        @Override
        public Enum<?> toSymbol( Object obj ) {
            return (Enum<?>) obj;
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
        public Long toInterval( Object obj ) {
            return toBigInt( obj );
        }


        @Override
        public Long toTimestamp( Object obj ) {
            return ((TimestampString) obj).getMillisSinceEpoch();
        }


        @Override
        public Integer toTime( Object obj ) {
            return ((TimeString) obj).getMillisOfDay();
        }


        @Override
        public Integer toDate( Object obj ) {
            return ((DateString) obj).getDaysSinceEpoch();
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
        public Byte toSmallInt( Object obj ) {
            return ((Integer) obj).byteValue();
        }


        @Override
        public Short toTinyInt( Object obj ) {
            return ((Integer) obj).shortValue();
        }


        @Override
        public Boolean toBoolean( Object obj ) {
            return (Boolean) obj;
        }

    }


    public enum ExternalToPolyphenyMapping implements TypeSpaceMapping<ExternalTypeDefinition, PolyphenyTypeDefinition> {
        INSTANCE;

        @Getter
        final Class<ExternalTypeDefinition> from = ExternalTypeDefinition.class;
        @Getter
        final Class<PolyphenyTypeDefinition> to = PolyphenyTypeDefinition.class;


        @Override
        public NlsString toJson( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public ByteString toMultimedia( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Object toObject( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public PolyList<?> toArray( Object obj, PolyType innerType ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Enum<?> toSymbol( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Object toAny( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Object toNull( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public ByteString toBinary( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public NlsString toVarchar( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public NlsString toChar( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public BigDecimal toInterval( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public TimestampString toTimestamp( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public TimeString toTime( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public DateString toDate( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public BigDecimal toDouble( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public BigDecimal toReal( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public BigDecimal toFloat( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public BigDecimal toDecimal( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Long toBigInt( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Integer toInteger( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Integer toSmallInt( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Integer toTinyInt( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        @Override
        public Boolean toBoolean( Object obj ) {
            throw new UsePolyTypeMappingException();
        }


        public static final class UsePolyTypeMappingException extends UnsupportedOperationException {

            public UsePolyTypeMappingException() {
                super( "To map types into the Polypheny TypeSpace use PolyTypeMapping." );
            }

        }

    }

}
