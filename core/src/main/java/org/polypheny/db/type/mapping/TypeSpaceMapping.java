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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * {@link TypeSpaceMapping} defines how types are mapped from one TypeSpace to another.
 * e.g. how the Mongo internal BsonTypes are mapped to the Polypheny type space and vice versa
 *
 * @param <F> definition of TypeSpace which is the source of data
 * @param <T> definition of TypeSpace which is the target of data
 */
public interface TypeSpaceMapping<F extends TypeDefinition<F>, T extends TypeDefinition<T>> {

    @Nonnull
    Class<T> getTo();

    @Nonnull
    Class<F> getFrom();


    /**
     * Checked manually on boot-up for every mapping.
     *
     * @return if the needed constraints for mappings are met.
     */
    default boolean checkConsistency() {
        Class<F> from = getFrom();
        Class<T> to = getTo();

        // additional check that from to conversion match;

        if ( Enum.class.isAssignableFrom( getClass() ) ) {
            return true;
        }

        throw new RuntimeException( String.format( "Mapping %s, which maps from %s to %s doesnt provided all needed conversion methods.",
                getClass().getSimpleName(), from.getSimpleName(), to.getSimpleName() ) );
    }


    default Object map( Object obj, AlgDataType type ) {
        PolyType componentType = type.getComponentType() != null ? type.getComponentType().getPolyType() : null;
        Pair<PolyType, PolyType> keyValueType = type.getKeyType() != null ? Pair.of( type.getKeyType().getPolyType(), type.getValueType().getPolyType() ) : null;

        return map( obj, type.getPolyType(), componentType, keyValueType );
    }


    @SuppressWarnings("unchecked")
    default Object map( Object obj, PolyType type, @Nullable PolyType componentType, @Nullable Pair<PolyType, PolyType> keyValueType ) {

        // used to assure that no null values are parsed,
        // but still the internal null representation is used
        if ( obj == null ) {
            return toNull( null );
        }

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


    Object toJson( Object obj );

    Object toMultimedia( Object obj );

    Object toObject( Object obj );

    Object toMap( Object obj, PolyType keyType, PolyType valueType );

    Object toArray( Object obj, PolyType innerType );

    Object toSymbol( Object obj );

    Object toAny( Object obj );

    Object toNull( Object obj );

    Object toBinary( Object obj );

    Object toVarchar( Object obj );

    Object toChar( Object obj );

    Object toInterval( Object obj );

    Object toTimestamp( Object obj );

    Object toTime( Object obj );

    Object toDate( Object obj );

    Object toDouble( Object obj );

    Object toReal( Object obj );

    Object toFloat( Object obj );

    Object toBigInt( Object obj );

    Object toDecimal( Object obj );

    Object toInteger( Object obj );

    Object toSmallInt( Object obj );

    Object toTinyInt( Object obj );

    Object toBoolean( Object obj );


    /**
     * This method provided the names of the methods, which transform from a source type convention to a target convetion
     */
    static String getMethodName( PolyType polyType ) {
        return "to" + getMethodTypeName( polyType );
    }


    static String getMethodTypeName( PolyType polyType ) {
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


    class UnsupportedTypeException extends RuntimeException {

        public UnsupportedTypeException( String conventionName, PolyType type ) {
            super( String.format( "Convention %s does not support type %s.", conventionName, type ) );
        }

    }

}
