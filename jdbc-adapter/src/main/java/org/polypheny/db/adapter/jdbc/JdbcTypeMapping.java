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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeMapping;
import org.polypheny.db.type.TypeMapping;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.PolySerializer;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public class JdbcTypeMapping extends TypeMapping<Object> {

    public final static JdbcTypeMapping INSTANCE = new JdbcTypeMapping();


    public JdbcTypeMapping() {
        // empty on purpose
    }


    @Override
    public Object fromJson( NlsString obj ) {
        return obj.getValue();
    }


    @Override
    public NlsString toJson( Object obj ) {
        return new NlsString( (String) obj, StandardCharsets.UTF_8.name(), Collation.IMPLICIT );
    }


    @Override
    public Object fromMultimedia( ByteString obj ) {
        return obj.getBytes();
    }


    @Override
    public ByteString toMultimedia( Object obj ) {
        return new ByteString( (byte[]) obj );
    }


    @Override
    public Object fromObject( Object obj ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object toObject( Object obj ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object fromMap( PolyMap<?, ?> obj, PolyType keyType, PolyType valueType ) {
        return PolySerializer.parseMap( obj );
    }


    @Override
    public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
        return PolySerializer.unparseMap( (String) obj );
    }


    @Override
    public Object fromArray( PolyList<?> obj, PolyType componentType ) {
        PolyList<Comparable<?>> list = new PolyList<>();
        obj.forEach( e -> list.add( (Comparable<?>) mapInto( e, componentType, null, null ) ) );
        return list;
    }


    @Override
    public PolyList<?> toArray( Object obj, PolyType componentType ) {
        PolyList<Comparable<?>> list = new PolyList<>();
        ((PolyList<?>) obj).forEach( e -> list.add( (Comparable<?>) mapOutOf( e, componentType, null, null ) ) );
        return list;
    }


    @Override
    public Object fromSymbol( Enum<?> obj ) {
        return obj;
    }


    @Override
    public Enum<?> toSymbol( Object obj ) {
        return (Enum<?>) obj;
    }


    @Override
    public Object fromAny( Object obj ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object toAny( Object obj ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object fromNull( Object obj ) {
        return obj;
    }


    @Override
    public Object toNull( Object obj ) {
        return obj;
    }


    @Override
    public Object fromBinary( ByteString obj ) {
        return obj.getBytes();
    }


    @Override
    public ByteString toBinary( Object obj ) {
        return new ByteString( (byte[]) obj );
    }


    @Override
    public Object fromVarchar( NlsString obj ) {
        return obj.getValue();
    }


    @Override
    public NlsString toVarchar( Object obj ) {
        return new NlsString( (String) obj, StandardCharsets.UTF_8.name(), Collation.IMPLICIT );
    }


    @Override
    public Object fromChar( NlsString obj ) {
        return fromVarchar( obj );
    }


    @Override
    public NlsString toChar( Object obj ) {
        return toVarchar( obj );
    }


    @Override
    public Object fromInterval( BigDecimal obj ) {
        return obj;
    }


    @Override
    public BigDecimal toInterval( Object obj ) {
        return (BigDecimal) obj;
    }


    @Override
    public Object fromTimestamp( TimestampString obj ) {
        return new java.sql.Timestamp( obj.getMillisSinceEpoch() - PolyTypeMapping.TIMEZONE_OFFSET );
    }


    @Override
    public TimestampString toTimestamp( Object obj ) {
        return TimestampString.fromMillisSinceEpoch( ((java.sql.Timestamp) obj).getTime() );
    }


    @Override
    public Object fromTime( TimeString obj ) {
        return new java.sql.Time( obj.getMillisOfDay() - PolyTypeMapping.TIMEZONE_OFFSET );
    }


    @Override
    public TimeString toTime( Object obj ) {
        return new TimeString( obj.toString() );
    }


    @Override
    public Object fromDate( DateString obj ) {
        return new java.sql.Date( obj.getMillisSinceEpoch() );
    }


    @Override
    public DateString toDate( Object obj ) {
        return new DateString( obj.toString() );
    }


    @Override
    public Object fromDouble( BigDecimal obj ) {
        return obj.doubleValue();
    }


    @Override
    public BigDecimal toDouble( Object obj ) {
        return BigDecimal.valueOf( (Double) obj );
    }


    @Override
    public Object fromReal( BigDecimal obj ) {
        return fromFloat( obj );
    }


    @Override
    public BigDecimal toReal( Object obj ) {
        return toFloat( obj );
    }


    @Override
    public Object fromFloat( BigDecimal obj ) {
        return obj.floatValue();
    }


    @Override
    public BigDecimal toFloat( Object obj ) {
        return BigDecimal.valueOf( (Float) obj );
    }


    @Override
    public Object fromDecimal( BigDecimal obj ) {
        return obj;
    }


    @Override
    public BigDecimal toDecimal( Object obj ) {
        return (BigDecimal) obj;
    }


    @Override
    public Object fromBigInt( BigDecimal obj ) {
        return obj.longValue();
    }


    @Override
    public BigDecimal toBigInt( Object obj ) {
        if ( obj instanceof Integer ) {
            return new BigDecimal( (Integer) obj );
        }
        return BigDecimal.valueOf( (Long) obj );
    }


    @Override
    public Object fromInteger( BigDecimal obj ) {
        return obj.intValue();
    }


    @Override
    public BigDecimal toInteger( Object obj ) {
        return new BigDecimal( (Integer) obj );
    }


    @Override
    public Object fromSmallInt( BigDecimal obj ) {
        return fromInteger( obj );
    }


    @Override
    public BigDecimal toSmallInt( Object obj ) {
        return toInteger( obj );
    }


    @Override
    public Object fromTinyInt( BigDecimal obj ) {
        return fromInteger( obj );
    }


    @Override
    public BigDecimal toTinyInt( Object obj ) {
        return toInteger( obj );
    }


    @Override
    public Object fromBoolean( Boolean obj ) {
        return obj;
    }


    @Override
    public Boolean toBoolean( Object obj ) {
        return (Boolean) obj;
    }


}
