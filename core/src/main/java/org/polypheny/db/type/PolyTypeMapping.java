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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.commons.lang.ArrayUtils;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolyCollections.PolyMap;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

@Slf4j
public class PolyTypeMapping {


    public static final int TIMEZONE_OFFSET = Calendar.getInstance().getTimeZone().getRawOffset();


    /**
     * Converter method, which should clearly map each value to its type and remove the uncertainty when handling the types
     * due to this mapping, it should require to only handle one type in the adapters when facing a PolyType
     * The types are matched to the most generic type, which is able to handle all PolyTypes, e.g BigDecimal for nearly all
     * numeric values
     *
     * @param value
     * @param type
     * @param typeName
     * @return
     */
    public static Comparable<?> convertPolyValue( Object value, AlgDataType type, PolyType typeName ) {

        if ( value == null ) {
            return null;
        }

        Comparable<?> converted = null;

        switch ( typeName ) {

            case BOOLEAN:
                // Boolean
                converted = handleBoolean( value );
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DECIMAL:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
                // BigDecimal
                converted = handleExactNumber( handleNumber( value ), typeName );
                break;
            case DATE:
                // DateString
                converted = handleDate( value );
                break;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                // TimeString
                converted = handleTime( value );
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // TimestampString
                converted = handleTimestamp( value );
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
                // BigDecimal
                converted = handleInterval( value );
                break;
            case SYMBOL:
            case ANY:
            case DISTINCT:
            case STRUCTURED:
            case OTHER:
            case CURSOR:
            case DYNAMIC_STAR:
            case GEOMETRY:
                // NlsString
                converted = handleGeneric( value );
                break;
            case CHAR:
            case VARCHAR:
            case JSON:
                // NlsString
                converted = handleString( value, type.getCharset(), type.getCollation() );
                break;
            case BINARY:
            case VARBINARY:
                // ByteString
                converted = handleByte( value );
                break;
            case NULL:
                throw new RuntimeException( "This should already been handled." );
            case ROW:
            case ARRAY:
            case MULTISET:
            case COLUMN_LIST:
                // PolyList
                converted = handleArray( value, type, typeName );
                break;
            case MAP:
                // PolyMap
                converted = handleMap( value, type );
                break;
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                // ByteString
                converted = handleMultimedia( value );
                break;
        }

        if ( converted != null ) {
            return converted;
        }

        throw new RuntimeException( String.format( "It was not possible to correctly handle the type: %s with value: %s", type, value ) );
    }


    private static Comparable<?> handleExactNumber( BigDecimal value, PolyType type ) {
        if ( value == null ) {
            return null;
        }
        switch ( type ) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return value.intValue();
            case BIGINT:
                return value.longValue();
        }
        return value;

    }


    public static Comparable<?> handleGeneric( Object value ) {
        if ( value instanceof Comparable<?> ) {
            return (Comparable<?>) value;
        }
        return null;
    }


    public static Boolean handleBoolean( Object value ) {
        if ( value instanceof Boolean ) {
            return (Boolean) value;
        }
        return null;
    }


    public static TimeString handleTime( Object value ) {
        if ( value instanceof TimeString ) {
            return (TimeString) value;
        } else if ( value instanceof Time ) {
            return new TimeString( value.toString() );
        } else if ( value instanceof Integer ) {
            return TimeString.fromMillisOfDay( (Integer) value );
        } else if ( value instanceof String ) {
            return new TimeString( (String) value );
        }
        return null;
    }


    public static TimestampString handleTimestamp( Object value ) {
        if ( value instanceof TimestampString ) {
            return (TimestampString) value;
        } else if ( value instanceof Timestamp ) {
            return TimestampString.fromMillisSinceEpoch( ((Timestamp) value).getTime() );
        } else if ( value instanceof Long ) {
            return TimestampString.fromMillisSinceEpoch( (Long) value );
        } else if ( value instanceof String ) {
            return new TimestampString( (String) value );
        }
        return null;
    }


    public static BigDecimal handleInterval( Object value ) {
        // this needs maybe changing
        if ( value instanceof BigDecimal ) {
            return (BigDecimal) value;
        } else if ( value instanceof Long ) {
            return new BigDecimal( (Long) value );
        } else if ( value instanceof Integer ) {
            return new BigDecimal( (Integer) value );
        } else if ( value instanceof String ) {
            logPossibleError( value, Long.class );
            return new BigDecimal( (String) value );
        }
        return null;
    }


    public static NlsString handleString( Object value, Charset charset, Collation collation ) {
        if ( value instanceof NlsString ) {
            return (NlsString) value;
        } else if ( value instanceof String ) {
            return new NlsString(
                    (String) value,
                    charset != null ? charset.name() : null,
                    collation );
        }
        return null;
    }


    public static ByteString handleMultimedia( Object value ) {
        // we need to consume the Stream,
        // even when we use an own type who is comparable at the comparing step we would need to consume the stream anyway
        // but indeed, this limits Multimedia types size for now...
        if ( value instanceof ByteString ) {
            return (ByteString) value;
        } else if ( value instanceof String ) {
            return ByteString.ofBase64( (String) value );
        } else if ( value instanceof InputStream ) {

            try {
                byte[] bytes = new byte[((InputStream) value).available()];
                ((InputStream) value).read( bytes );
                return new ByteString( bytes );
            } catch ( IOException e ) {
                throw new RuntimeException( "Error while unpacking stream." );
            }
        }
        return null;
    }


    public static PolyMap<Comparable<?>, Comparable<?>> handleMap( Object value, AlgDataType type ) {
        AlgDataType keyType = type.getKeyType();
        AlgDataType valueType = type.getValueType();
        if ( value instanceof Map<?, ?> ) {
            PolyMap<Comparable<?>, Comparable<?>> polyMap = new PolyMap<>();
            ((PolyMap<?, ?>) value).forEach( ( k, v ) -> polyMap.put( convertPolyValue( k, keyType, keyType.getPolyType() ), convertPolyValue( v, valueType, valueType.getPolyType() ) ) );
            return polyMap;
        }
        return null;
    }


    public static PolyList<? extends Comparable<?>> handleArray( Object value, AlgDataType type, PolyType typeName ) {
        AlgDataType elementType = type.getComponentType();
        if ( value instanceof PolyList<?> ) {
            return ((PolyList<?>) value)
                    .stream()
                    .map( e -> convertPolyValue( e, elementType, typeName ) )
                    .collect( Collectors.toCollection( PolyList::new ) );
        } else if ( value instanceof List<?> ) {
            return ((List<?>) value)
                    .stream()
                    .map( e -> convertPolyValue( e, elementType, elementType.getPolyType() ) )
                    .collect( Collectors.toCollection( PolyList::new ) );
        } else if ( value instanceof Comparable<?>[] ) {
            return Arrays.stream( (Comparable<?>[]) value )
                    .map( e -> convertPolyValue( e, elementType, elementType.getPolyType() ) )
                    .collect( Collectors.toCollection( PolyList::new ) );
        }
        return null;
    }


    public static ByteString handleByte( Object value ) {
        if ( value instanceof ByteString ) {
            return (ByteString) value;
        } else if ( value instanceof Byte ) {
            return new ByteString( new byte[]{ ((Byte) value) } );
        } else if ( value instanceof byte[] ) {
            return new ByteString( (byte[]) value );
        } else if ( value instanceof Byte[] ) {
            return new ByteString( ArrayUtils.toPrimitive( (Byte[]) value ) );
        }
        return null;
    }


    public static DateString handleDate( Object value ) {
        if ( value instanceof DateString ) {
            return (DateString) value;
        } else if ( value instanceof Calendar ) {
            return DateString.fromCalendarFields( (Calendar) value );
        } else if ( value instanceof Date ) {
            return new DateString( value.toString() );
        } else if ( value instanceof Integer ) {
            return DateString.fromDaysSinceEpoch( (Integer) value );
        } else if ( value instanceof String ) {
            return new DateString( (String) value );
        }
        return null;
    }


    public static BigDecimal handleNumber( Object value ) {

        if ( value instanceof BigDecimal ) {
            return (BigDecimal) value;
        } else if ( value instanceof Integer ) {
            return new BigDecimal( (Integer) value );
        } else if ( value instanceof Long ) {
            return new BigDecimal( (Long) value );
        } else if ( value instanceof Double ) {
            return BigDecimal.valueOf( (Double) value );
        } else if ( value instanceof Float ) {
            return BigDecimal.valueOf( (Float) value );
        } else if ( value instanceof Short ) {
            return new BigDecimal( (Short) value );
        } else if ( value instanceof Byte ) {
            return new BigDecimal( (Byte) value );
        } else if ( value instanceof String ) {
            logPossibleError( value, BigDecimal.class );
            return new BigDecimal( (String) value );
        }

        return null;
    }


    private static void logPossibleError( Object value, Class<?> clazz ) {
        log.warn( String.format( "value:%s to %s casting.", value, clazz.getSimpleName() ) );
    }


    public static AlgDataType from( Rep type, Rep componentType ) {
        switch ( type ) {
            case PRIMITIVE_BOOLEAN:
            case BOOLEAN:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.BOOLEAN );
            case PRIMITIVE_BYTE:
            case PRIMITIVE_SHORT:
            case SHORT:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.SMALLINT );
            case PRIMITIVE_CHAR:
            case CHARACTER:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.CHAR );
            case PRIMITIVE_INT:
            case INTEGER:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.INTEGER );
            case PRIMITIVE_LONG:
            case LONG:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.BIGINT );
            case PRIMITIVE_FLOAT:
            case FLOAT:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.FLOAT );
            case PRIMITIVE_DOUBLE:
            case DOUBLE:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.DOUBLE );
            case BYTE_STRING:
            case BYTE:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.BINARY, 255 );
            case NUMBER:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.DECIMAL );
            case JAVA_SQL_TIME:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.TIME, 3 );
            case JAVA_SQL_TIMESTAMP:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.TIMESTAMP, 3 );
            case JAVA_SQL_DATE:
            case JAVA_UTIL_DATE:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.DATE );
            case STRING:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 );
            case OBJECT:
            case STRUCT:
                return new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.OTHER );
            case MULTISET:
            case ARRAY:
                return new ArrayType( from( componentType, null ), true );
        }
        throw new RuntimeException( "Could not create Type for Jdbc Rep: " + type );
    }


}