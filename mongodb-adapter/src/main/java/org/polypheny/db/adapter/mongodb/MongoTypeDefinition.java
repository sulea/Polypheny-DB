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

package org.polypheny.db.adapter.mongodb;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.mapping.PolyphenyTypeDefinition;
import org.polypheny.db.type.mapping.TypeDefinition;
import org.polypheny.db.type.mapping.TypeSpaceMapping;
import org.polypheny.db.type.mapping.TypeSpaceMapping.UnsupportedTypeException;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.PolySerializer;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public enum MongoTypeDefinition implements TypeDefinition<MongoTypeDefinition> {
    INSTANCE;


    @Override
    public TypeSpaceMapping<MongoTypeDefinition, PolyphenyTypeDefinition> getToPolyphenyMapping() {
        return MongoToPolyphenyMapping.INSTANCE;
    }


    @Override
    public TypeSpaceMapping<PolyphenyTypeDefinition, MongoTypeDefinition> getFromPolyphenyMapping() {
        return PolyphenyToMongoMapping.INSTANCE;
    }


    @Override
    public List<Class<?>> getMappingClasses( PolyType type ) {
        return Collections.singletonList( getMappingClass( type ) );
    }


    public Class<?> getMappingClass( PolyType type ) {
        switch ( type ) {
            case BOOLEAN:
                return BsonBoolean.class;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case DATE:
                return BsonInt32.class;
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
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
                return BsonInt64.class;
            case DECIMAL:
                return BsonDecimal128.class;
            case FLOAT:
            case REAL:
            case DOUBLE:
                return BsonDouble.class;
            case CHAR:
            case VARCHAR:
                return BsonString.class;
            case BINARY:
            case VARBINARY:
            case VIDEO:
            case SOUND:
            case IMAGE:
            case FILE:
                return Object.class;
            case NULL:
                return BsonNull.class;
            case ANY:
                return BsonValue.class;
            case GEOMETRY:
            case DYNAMIC_STAR:
            case CURSOR:
            case OTHER:
            case STRUCTURED:
            case DISTINCT:
                throw new UnsupportedTypeException( "Mongo", type );
            case SYMBOL:
                return BsonSymbol.class;
            case MULTISET:
            case ARRAY:
            case COLUMN_LIST:
            case ROW:
                return BsonArray.class;
            case MAP:
            case JSON:
                return BsonDocument.class;
        }

        throw new UnsupportedTypeException( "Mongo", type );
    }


    public enum PolyphenyToMongoMapping implements TypeSpaceMapping<PolyphenyTypeDefinition, MongoTypeDefinition> {
        INSTANCE;

        @Getter
        final Class<PolyphenyTypeDefinition> from = PolyphenyTypeDefinition.class;
        @Getter
        final Class<MongoTypeDefinition> to = MongoTypeDefinition.class;


        @Override
        public BsonDocument toJson( Object obj ) {
            return Document.parse( obj.toString() ).toBsonDocument( BsonDocument.class, Bson.DEFAULT_CODEC_REGISTRY );
        }


        @Override
        public Object toMultimedia( Object obj ) {
            if ( obj instanceof InputStream ) {
                return obj;
            }
            return new BsonBinary( ((ByteString) obj).getBytes() );
        }


        @Override
        public Object toObject( Object obj ) {
            throw new UnsupportedTypeException( "Mongo", PolyType.OTHER );
        }


        @Override
        public Object toMap( Object obj, PolyType keyType, PolyType valueType ) {
            BsonDocument doc = new BsonDocument();
            if ( keyType == PolyType.VARCHAR || keyType == PolyType.CHAR ) {
                ((PolyMap<?, ?>) obj)
                        .forEach( ( k, v ) ->
                                doc.put( ((NlsString) k).getValue(), (BsonValue) map( v, valueType, null, null ) ) );
                return doc;
            }
            return new BsonString( PolySerializer.serializeMap( (PolyMap<?, ?>) obj ) );
        }


        @Override
        public BsonArray toArray( Object obj, PolyType innerType ) {
            BsonArray array = new BsonArray();
            ((PolyList<?>) obj).forEach( e -> array.add( (BsonValue) map( e, innerType, null, null ) ) );
            return array;
        }


        @Override
        public BsonSymbol toSymbol( Object obj ) {
            return new BsonSymbol( ((NlsString) obj).getValue() );
        }


        @Override
        public Object toAny( Object obj ) {
            throw new UnsupportedTypeException( "Mongo", PolyType.ANY );
        }


        @Override
        public BsonNull toNull( Object obj ) {
            return new BsonNull();
        }


        @Override
        public BsonBinary toBinary( Object obj ) {
            return new BsonBinary( ((ByteString) obj).getBytes() );
        }


        @Override
        public BsonString toVarchar( Object obj ) {
            return new BsonString( ((NlsString) obj).getValue() );
        }


        @Override
        public BsonString toChar( Object obj ) {
            return toVarchar( obj );
        }


        @Override
        public BsonInt64 toInterval( Object obj ) {
            return new BsonInt64( (Long) obj );
        }


        @Override
        public BsonInt64 toTimestamp( Object obj ) {
            return new BsonInt64( ((TimestampString) obj).getMillisSinceEpoch() );
        }


        @Override
        public BsonInt32 toTime( Object obj ) {
            return new BsonInt32( ((TimeString) obj).getMillisOfDay() );
        }


        @Override
        public BsonInt32 toDate( Object obj ) {
            return new BsonInt32( ((DateString) obj).getDaysSinceEpoch() );
        }


        @Override
        public BsonDecimal128 toDouble( Object obj ) {
            return toDecimal( obj );
        }


        @Override
        public BsonDecimal128 toReal( Object obj ) {
            return toDecimal( obj );
        }


        @Override
        public Object toFloat( Object obj ) {
            return toDouble( obj );
        }


        @Override
        public BsonInt64 toBigInt( Object obj ) {
            return new BsonInt64( (Long) obj );
        }


        @Override
        public BsonDecimal128 toDecimal( Object obj ) {
            return new BsonDecimal128( new Decimal128( (BigDecimal) obj ) );
        }


        @Override
        public BsonInt32 toInteger( Object obj ) {
            return new BsonInt32( (Integer) obj );
        }


        @Override
        public BsonInt32 toSmallInt( Object obj ) {
            return toInteger( obj );
        }


        @Override
        public BsonInt32 toTinyInt( Object obj ) {
            return toInteger( obj );
        }


        @Override
        public BsonBoolean toBoolean( Object obj ) {
            return new BsonBoolean( (Boolean) obj );
        }
    }


    public enum MongoToPolyphenyMapping implements TypeSpaceMapping<MongoTypeDefinition, PolyphenyTypeDefinition> {
        INSTANCE;

        @Getter
        public final Class<MongoTypeDefinition> from = MongoTypeDefinition.class;

        @Getter
        public final Class<PolyphenyTypeDefinition> to = PolyphenyTypeDefinition.class;


        @Override
        public NlsString toJson( Object obj ) {
            return TypeDefinition.getNlsString( ((BsonDocument) obj).toJson() );
        }


        @Override
        public InputStream toMultimedia( Object obj ) {
            if ( obj instanceof BsonBinary ) {
                return new PushbackInputStream( new ByteArrayInputStream( ((BsonBinary) obj).getData() ) );
            }
            throw new UnsupportedTypeException( "Mongo", PolyType.FILE );
        }


        @Override
        public Object toObject( Object obj ) {
            throw new UnsupportedTypeException( "Mongo", PolyType.OTHER );
        }


        @Override
        public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
            if ( keyType == PolyType.VARCHAR || keyType == PolyType.CHAR ) {
                PolyMap<Comparable<?>, Comparable<?>> map = new PolyMap<>();
                ((BsonDocument) obj)
                        .forEach( ( k, v ) -> map.put( toVarchar( k ), (Comparable<?>) map( v, valueType, null, null ) ) );
                return map;
            }

            return PolySerializer.deserializeMap( String.valueOf( obj ) );
        }


        @Override
        public PolyList<?> toArray( Object obj, PolyType innerType ) {
            PolyList<Comparable<?>> list = new PolyList<>();
            ((BsonArray) obj).forEach( e -> list.add( (Comparable<?>) map( e, innerType, null, null ) ) );
            return list;
        }


        @Override
        public Object toSymbol( Object obj ) {
            throw new UnsupportedTypeException( "Mongo", PolyType.SYMBOL );
        }


        @Override
        public Object toAny( Object obj ) {
            throw new UnsupportedTypeException( "Mongo", PolyType.ANY );
        }


        @Override
        public Object toNull( Object obj ) {
            return null;
        }


        @Override
        public Object toBinary( Object obj ) {
            return new ByteString( ((BsonBinary) obj).getData() );
        }


        @Override
        public NlsString toVarchar( Object obj ) {
            return TypeDefinition.getNlsString( ((BsonString) obj).getValue() );
        }


        @Override
        public NlsString toChar( Object obj ) {
            return toVarchar( obj );
        }


        @Override
        public Long toInterval( Object obj ) {
            return ((BsonInt64) obj).getValue();
        }


        @Override
        public TimestampString toTimestamp( Object obj ) {
            return TimestampString.fromMillisSinceEpoch( ((BsonInt64) obj).getValue() );
        }


        @Override
        public TimeString toTime( Object obj ) {
            return TimeString.fromMillisOfDay( ((BsonInt32) obj).intValue() );
        }


        @Override
        public DateString toDate( Object obj ) {
            return DateString.fromDaysSinceEpoch( ((BsonInt32) obj).intValue() );
        }


        @Override
        public BigDecimal toDouble( Object obj ) {
            return toDecimal( obj );
        }


        @Override
        public BigDecimal toReal( Object obj ) {
            return toDecimal( obj );
        }


        @Override
        public BigDecimal toFloat( Object obj ) {
            return toDecimal( obj );
        }


        @Override
        public Long toBigInt( Object obj ) {
            return ((BsonInt64) obj).getValue();
        }


        @Override
        public BigDecimal toDecimal( Object obj ) {
            return ((BsonDecimal128) obj).decimal128Value().bigDecimalValue();
        }


        @Override
        public Integer toInteger( Object obj ) {
            return ((BsonInt32) obj).getValue();
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
            return ((BsonBoolean) obj).getValue();
        }
    }


}
