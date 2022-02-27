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

package org.polypheny.db.adapter.file.algebra;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.mapping.PolyphenyTypeDefinition;
import org.polypheny.db.type.mapping.TypeDefinition;
import org.polypheny.db.type.mapping.TypeSpaceMapping;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyBson;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.PolySerializer;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public enum FileTypeDefinition implements TypeDefinition<FileTypeDefinition> {
    INSTANCE;


    @Override
    public TypeSpaceMapping<FileTypeDefinition, PolyphenyTypeDefinition> getToPolyphenyMapping() {
        return FileToPolyphenyMapping.INSTANCE;
    }


    @Override
    public TypeSpaceMapping<PolyphenyTypeDefinition, FileTypeDefinition> getFromPolyphenyMapping() {
        return PolyphenyToFileMapping.INSTANCE;
    }


    @Override
    public List<Class<?>> getMappingClasses( PolyType type ) {
        switch ( type ) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case NULL:
            case ANY:
            case SYMBOL:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                return Collections.singletonList( String.class );
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
            case JSON:
                return Collections.singletonList( Object.class );
            default:
                throw new UnsupportedOperationException();
        }
    }


    public enum FileToPolyphenyMapping implements TypeSpaceMapping<FileTypeDefinition, PolyphenyTypeDefinition> {
        INSTANCE;


        @Nonnull
        @Override
        public Class<PolyphenyTypeDefinition> getTo() {
            return PolyphenyTypeDefinition.class;
        }


        @Nonnull
        @Override
        public Class<FileTypeDefinition> getFrom() {
            return FileTypeDefinition.class;
        }


        @Override
        public PolyBson toJson( Object obj ) {
            return PolyBson.parse( (String) obj );
        }


        @Override
        public ByteString toMultimedia( Object obj ) {
            return new ByteString( (byte[]) obj );
        }


        @Override
        public Object toObject( Object obj ) {
            return obj;
        }


        @Override
        public Object toMap( Object obj, PolyType keyType, PolyType valueType ) {
            return PolySerializer.deserializeMap( (String) obj );
        }


        @Override
        public Object toArray( Object obj, PolyType innerType ) {
            return PolySerializer.deserializeArray( innerType, -1L, (String) obj );
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
        public Long toInterval( Object obj ) {
            return (Long) obj;
        }


        @Override
        public TimestampString toTimestamp( Object obj ) {
            return TimestampString.fromMillisSinceEpoch( (Long) obj );
        }


        @Override
        public TimeString toTime( Object obj ) {
            return TimeString.fromMillisOfDay( (Integer) obj );
        }


        @Override
        public DateString toDate( Object obj ) {
            return DateString.fromDaysSinceEpoch( (Integer) obj );
        }


        @Override
        public Double toDouble( Object obj ) {
            return ((Double) obj);
        }


        @Override
        public Double toReal( Object obj ) {
            return toDouble( obj );
        }


        @Override
        public Double toFloat( Object obj ) {
            return toDouble( obj );
        }


        @Override
        public Long toBigInt( Object obj ) {
            return (Long) obj;
        }


        @Override
        public Object toDecimal( Object obj ) {
            return new BigDecimal( (String) obj );
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
        public Object toTinyInt( Object obj ) {
            return toInteger( obj );
        }


        @Override
        public Boolean toBoolean( Object obj ) {
            return (Boolean) obj;
        }
    }


    public enum PolyphenyToFileMapping implements TypeSpaceMapping<PolyphenyTypeDefinition, FileTypeDefinition> {
        INSTANCE;


        @Nonnull
        @Override
        public Class<FileTypeDefinition> getTo() {
            return FileTypeDefinition.class;
        }


        @Nonnull
        @Override
        public Class<PolyphenyTypeDefinition> getFrom() {
            return PolyphenyTypeDefinition.class;
        }


        @Override
        public String toJson( Object obj ) {
            return (String) obj;
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
        public Object toMap( Object obj, PolyType keyType, PolyType valueType ) {
            return PolySerializer.serializeMap( (PolyMap<?, ?>) obj );
        }


        @Override
        public Object toArray( Object obj, PolyType innerType ) {
            return PolySerializer.serializeArray( obj );
        }


        @Override
        public Object toSymbol( Object obj ) {
            return obj.toString();
        }


        @Override
        public Object toAny( Object obj ) {
            return obj.toString();
        }


        @Override
        public Object toNull( Object obj ) {
            return obj.toString();
        }


        @Override
        public Object toBinary( Object obj ) {
            return obj.toString();
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
            return (Long) obj;
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
        public Object toDouble( Object obj ) {
            return obj;
        }


        @Override
        public Object toReal( Object obj ) {
            return toDouble( obj );
        }


        @Override
        public Object toFloat( Object obj ) {
            return toDouble( obj );
        }


        @Override
        public Long toBigInt( Object obj ) {
            return (Long) obj;
        }


        @Override
        public String toDecimal( Object obj ) {
            return obj.toString();
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
