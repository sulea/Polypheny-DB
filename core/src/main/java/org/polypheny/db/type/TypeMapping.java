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

package org.polypheny.db.type;

import java.math.BigDecimal;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

/**
 * This class should be used to provide the needed internal and external mappings for each adapter to
 * its own internal representation of the clearly defined PolyTypes as seen in {@link PolyTypeMapping#convertPolyValue(Object, AlgDataType, PolyType)}
 * Has to be of type {@link Function1} to create Expression from it later on, luckily this is mostly similar
 * to the default {@link Function}.
 *
 * Subclasses, which implement this class need to provide non args constructor, if this poses a problem in the future it can be solved via a registry.
 *
 * @param <T>
 */
public abstract class TypeMapping<T> {

    public Object mapOutOf( T obj, AlgDataType type ) {
        PolyType componentType = type.getComponentType() != null ? type.getComponentType().getPolyType() : null;
        Pair<PolyType, PolyType> keyValueType = type.getKeyType() != null ? Pair.of( type.getKeyType().getPolyType(), type.getValueType().getPolyType() ) : null;

        return mapOutOf( obj, type.getPolyType(), componentType, keyValueType );
    }


    public Object mapOutOf( T obj, PolyType type, @Nullable PolyType componentType, @Nullable Pair<PolyType, PolyType> keyValueType ) {

        switch ( type ) {
            case BOOLEAN:
                return toBoolean( obj );
            case TINYINT:
                return toTinyInt( obj );
            case SMALLINT:
                return toSmallInt( obj );
            case INTEGER:
                return toInteger( obj );
            case BIGINT:
                return toBigInt( obj );
            case DECIMAL:
                return toDecimal( obj );
            case FLOAT:
                return toFloat( obj );
            case REAL:
                return toReal( obj );
            case DOUBLE:
                return toDouble( obj );
            case DATE:
                return toDate( obj );
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return toTime( obj );
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return toTimestamp( obj );
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
                return toInterval( obj );
            case CHAR:
                return toChar( obj );
            case VARCHAR:
                return toVarchar( obj );
            case BINARY:
            case VARBINARY:
                return toBinary( obj );
            case NULL:
                return toNull( obj );
            case ANY:
                return toAny( obj );
            case SYMBOL:
                return toSymbol( obj );
            case ROW:
            case MULTISET:
            case ARRAY:
                assert componentType != null;
                return toArray( obj, componentType );
            case MAP:
                assert keyValueType != null;
                return toMap( obj, keyValueType.getKey(), keyValueType.getValue() );
            case DISTINCT:
            case STRUCTURED:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                return toObject( obj );
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return toMultimedia( obj );
            case JSON:
                return toJson( obj );
        }

        throw new RuntimeException( "No mapping provided for type: " + type );
    }


    public T mapInto( T obj, AlgDataType type ) {
        PolyType componentType = type.getComponentType() != null ? type.getComponentType().getPolyType() : null;
        Pair<PolyType, PolyType> keyValueType = type.getKeyType() != null ? Pair.of( type.getKeyType().getPolyType(), type.getValueType().getPolyType() ) : null;

        return mapInto(
                obj,
                type.getPolyType(),
                componentType,
                keyValueType );
    }


    public T mapInto( T obj, PolyType type, @Nullable PolyType componentType, @Nullable Pair<PolyType, PolyType> keyValueType ) {
        switch ( type ) {
            case BOOLEAN:
                return fromBoolean( (Boolean) obj );
            case TINYINT:
                return fromTinyInt( (BigDecimal) obj );
            case SMALLINT:
                return fromSmallInt( (BigDecimal) obj );
            case INTEGER:
                return fromInteger( (BigDecimal) obj );
            case BIGINT:
                return fromBigInt( (BigDecimal) obj );
            case DECIMAL:
                return fromDecimal( (BigDecimal) obj );
            case FLOAT:
                return fromFloat( (BigDecimal) obj );
            case REAL:
                return fromReal( (BigDecimal) obj );
            case DOUBLE:
                return fromDouble( (BigDecimal) obj );
            case DATE:
                return fromDate( (DateString) obj );
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return fromTime( (TimeString) obj );
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return fromTimestamp( (TimestampString) obj );
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
                return fromInterval( (BigDecimal) obj );
            case CHAR:
                return fromChar( (NlsString) obj );
            case VARCHAR:
                return fromVarchar( (NlsString) obj );
            case BINARY:
            case VARBINARY:
                return fromBinary( (ByteString) obj );
            case NULL:
                return fromNull( obj );
            case ANY:
                return fromAny( obj );
            case SYMBOL:
                return fromSymbol( (Enum<?>) obj );
            case ROW:
            case MULTISET:
            case ARRAY:
                assert componentType != null;
                return fromArray( (PolyList<?>) obj, componentType );
            case MAP:
                assert keyValueType != null;
                return fromMap( (PolyMap<?, ?>) obj, keyValueType.getKey(), keyValueType.getValue() );
            case DISTINCT:
            case STRUCTURED:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                return fromObject( obj );
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return fromMultimedia( (ByteString) obj );
            case JSON:
                return fromJson( (NlsString) obj );
        }

        throw new RuntimeException( "No mapping provided for type: " + type );
    }


    public abstract T fromJson( NlsString obj );

    public abstract NlsString toJson( T obj );

    // questionable if this is needed for InputStream
    public abstract T fromMultimedia( ByteString obj );

    public abstract ByteString toMultimedia( T obj );

    public abstract T fromObject( Object obj );

    public abstract Object toObject( T obj );

    public abstract T fromMap( PolyMap<?, ?> obj, PolyType innerType, PolyType valueType );

    public abstract PolyMap<?, ?> toMap( T obj, PolyType keyType, PolyType valueType );

    public abstract T fromArray( PolyList<?> obj, PolyType innerType );

    public abstract PolyList<?> toArray( T obj, PolyType innerType );

    public abstract T fromSymbol( Enum<?> obj );

    public abstract Enum<?> toSymbol( T obj );

    public abstract T fromAny( Object obj );

    public abstract Object toAny( T obj );

    public abstract T fromNull( Object obj );

    public abstract Object toNull( T obj );

    public abstract T fromBinary( ByteString obj );

    public abstract ByteString toBinary( T obj );

    public abstract T fromVarchar( NlsString obj );

    public abstract NlsString toVarchar( T obj );

    public abstract T fromChar( NlsString obj );

    public abstract NlsString toChar( T obj );

    public abstract T fromInterval( BigDecimal obj );

    public abstract BigDecimal toInterval( T obj );

    public abstract T fromTimestamp( TimestampString obj );

    public abstract TimestampString toTimestamp( T obj );

    public abstract T fromTime( TimeString obj );

    public abstract TimeString toTime( T obj );

    public abstract T fromDate( DateString obj );

    public abstract DateString toDate( T obj );

    public abstract T fromDouble( BigDecimal obj );

    public abstract BigDecimal toDouble( T obj );

    public abstract T fromReal( BigDecimal obj );

    public abstract BigDecimal toReal( T obj );

    public abstract T fromFloat( BigDecimal obj );

    public abstract BigDecimal toFloat( T obj );

    public abstract T fromDecimal( BigDecimal obj );

    public abstract BigDecimal toDecimal( T obj );

    public abstract T fromBigInt( BigDecimal obj );

    public abstract BigDecimal toBigInt( Object obj );

    public abstract T fromInteger( BigDecimal obj );

    public abstract BigDecimal toInteger( T obj );

    public abstract T fromSmallInt( BigDecimal obj );

    public abstract BigDecimal toSmallInt( T obj );

    public abstract T fromTinyInt( BigDecimal obj );

    public abstract BigDecimal toTinyInt( T obj );

    public abstract T fromBoolean( Boolean obj );

    public abstract Boolean toBoolean( T obj );


    public boolean needsMapping( AlgDataType type, Class<?> internalReturnClass ) {
        // this defines if a type needs mapping given a class,
        // this should allow to skip mapping into the Polypheny defined
        // type space and to the external if the adapter already
        // conforms to it;
        // also to remove mapping if the adapter already uses the Polypheny type space
        // internally.

        return true;
    }


    /**
     * This method provided the names of the methods, which transform from a source type convention to a target convetion
     */
    public static String getToMethodName( PolyType polyType ) {
        return "to" + getMethodName( polyType );
    }


    public static String getFromMethodName( PolyType polyType ) {
        return "from" + getMethodName( polyType );
    }


    private static String getMethodName( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return "Boolean";
            case TINYINT:
                return "TinyInt";
            case SMALLINT:
                return "SmallInt";
            case INTEGER:
                return "Integer";
            case BIGINT:
                return "BigInt";
            case DECIMAL:
                return "Decimal";
            case FLOAT:
                return "Float";
            case REAL:
                return "Real";
            case DOUBLE:
                return "Double";
            case DATE:
                return "Date";
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return "Time";
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "Timestamp";
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
                return "Interval";
            case CHAR:
                return "Char";
            case VARCHAR:
                return "Varchar";
            case BINARY:
            case VARBINARY:
                return "Binary";
            case NULL:
                return "Null";
            case ANY:
                return "Any";
            case SYMBOL:
                return "Symbol";
            case ROW:
            case MULTISET:
            case ARRAY:
                return "Array";
            case MAP:
                return "Map";
            case DISTINCT:
            case STRUCTURED:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                return "Object";
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return "Multimedia";
            case JSON:
                return "Json";
        }

        throw new RuntimeException( "No suiting method found." );
    }

}
