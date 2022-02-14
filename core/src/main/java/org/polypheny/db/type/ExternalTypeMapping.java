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

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


public class ExternalTypeMapping extends TypeMapping<Object> {

    public final static ExternalTypeMapping INSTANCE = new ExternalTypeMapping();


    private ExternalTypeMapping() {
        // empty on purpose
    }


    @Override
    public Object fromJson( NlsString obj ) {
        return obj.getValue();
    }


    @Override
    public NlsString toJson( Object obj ) {
        return PolyTypeMapping.handleString( obj, Charset.defaultCharset(), Collation.IMPLICIT );
    }


    @Override
    public Object fromMultimedia( ByteString obj ) {
        return new ByteArrayInputStream( obj.getBytes() );
    }


    @Override
    public ByteString toMultimedia( Object obj ) {
        return PolyTypeMapping.handleMultimedia( obj );
    }


    @Override
    public Object fromObject( Object obj ) {
        return obj;
    }


    @Override
    public Object toObject( Object obj ) {
        return PolyTypeMapping.handleGeneric( obj );
    }


    @Override
    public PolyMap<?, ?> toMap( Object obj, PolyType keyType, PolyType valueType ) {
        PolyMap<Comparable<?>, Comparable<?>> map = new PolyMap<>();
        assert keyType != PolyType.MAP && keyType != PolyType.ARRAY && valueType != PolyType.MAP && valueType != PolyType.ARRAY;
        ((PolyMap<Comparable<?>, Comparable<?>>) obj)
                .forEach( ( key, value ) -> map.put( PolyTypeMapping.convertPolyValue( key, null, keyType ), PolyTypeMapping.convertPolyValue( value, null, valueType ) ) );
        return map;
    }


    @Override
    public PolyList<?> toArray( Object obj, PolyType componentType ) {
        assert componentType != PolyType.MAP && componentType != PolyType.ARRAY;
        PolyList<Comparable<?>> list = new PolyList<>();
        ((PolyList<Comparable<?>>) obj).forEach( e -> PolyTypeMapping.convertPolyValue( e, null, componentType ) );
        return list;
    }


    @Override
    public Enum<?> toSymbol( Object obj ) {
        return (Enum<?>) obj;
    }


    @Override
    public Object fromAny( Object obj ) {
        return obj;
    }


    @Override
    public Object toAny( Object obj ) {
        return obj;
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
        return PolyTypeMapping.handleByte( obj );
    }


    @Override
    public Object fromVarchar( NlsString obj ) {
        return obj.getValue();
    }


    @Override
    public NlsString toVarchar( Object obj ) {
        return PolyTypeMapping.handleString( obj, Charset.defaultCharset(), Collation.IMPLICIT );
    }


    @Override
    public Object fromChar( NlsString obj ) {
        return obj.getValue();
    }


    @Override
    public NlsString toChar( Object obj ) {
        return toVarchar( obj );
    }


    @Override
    public Object fromInterval( BigDecimal obj ) {
        return obj.longValue();
    }


    @Override
    public BigDecimal toInterval( Object obj ) {
        return PolyTypeMapping.handleInterval( obj );
    }


    @Override
    public Object fromTimestamp( TimestampString obj ) {
        return obj.getMillisSinceEpoch();
    }


    @Override
    public TimestampString toTimestamp( Object obj ) {
        return PolyTypeMapping.handleTimestamp( obj );
    }


    @Override
    public Object fromTime( TimeString obj ) {
        return obj.getMillisOfDay();
    }


    @Override
    public TimeString toTime( Object obj ) {
        return PolyTypeMapping.handleTime( obj );
    }


    @Override
    public Object fromDate( DateString obj ) {
        return obj.getMillisSinceEpoch();
    }


    @Override
    public DateString toDate( Object obj ) {
        return PolyTypeMapping.handleDate( obj );
    }


    @Override
    public Object fromDouble( BigDecimal obj ) {
        return obj.doubleValue();
    }


    @Override
    public BigDecimal toDouble( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromReal( BigDecimal obj ) {
        return obj.floatValue();
    }


    @Override
    public BigDecimal toReal( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromFloat( BigDecimal obj ) {
        return obj.floatValue();
    }


    @Override
    public BigDecimal toFloat( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromDecimal( BigDecimal obj ) {
        return obj;
    }


    @Override
    public BigDecimal toDecimal( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromBigInt( BigDecimal obj ) {
        return obj.longValue();
    }


    @Override
    public BigDecimal toBigInt( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromInteger( BigDecimal obj ) {
        return obj.intValue();
    }


    @Override
    public BigDecimal toInteger( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromSmallInt( BigDecimal obj ) {
        return obj.byteValue();
    }


    @Override
    public BigDecimal toSmallInt( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromTinyInt( BigDecimal obj ) {
        return obj.byteValue();
    }


    @Override
    public BigDecimal toTinyInt( Object obj ) {
        return PolyTypeMapping.handleNumber( obj );
    }


    @Override
    public Object fromBoolean( Boolean obj ) {
        return obj;
    }


    @Override
    public Boolean toBoolean( Object obj ) {
        return (Boolean) obj;
    }


    @Override
    public Object fromSymbol( Enum<?> obj ) {
        return obj;
    }


    @Override
    public Object fromArray( PolyList<?> obj, PolyType componentType ) {
        PolyList<Comparable<?>> list = new PolyList<>();
        obj.forEach( e -> list.add( (Comparable<?>) mapInto( e, componentType, null, null ) ) );
        return list;
    }


    @Override
    public Object fromMap( PolyMap<?, ?> obj, PolyType keyType, PolyType valueType ) {
        PolyMap<Comparable<?>, Comparable<?>> map = new PolyMap<>();
        obj.forEach( ( key, value ) ->
                map.put(
                        (Comparable<?>) mapInto( key, keyType, null, null ),
                        (Comparable<?>) mapInto( value, valueType, null, null ) ) );
        return map;
    }

}
