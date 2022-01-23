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

package org.polypheny.db.serialize;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;
import org.apache.calcite.avatica.util.ByteString;
import org.nustaq.serialization.FSTConfiguration;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationImpl;
import org.polypheny.db.algebra.AlgCollationImpl.AlgCollationSerializer;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.AlgFieldCollationSerializer;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.LogicalAggregate.SerializableAggregate;
import org.polypheny.db.algebra.logical.LogicalFilter.SerializableFilter;
import org.polypheny.db.algebra.logical.LogicalJoin.SerializableJoin;
import org.polypheny.db.algebra.logical.LogicalProject.SerializableProject;
import org.polypheny.db.algebra.logical.LogicalSort.SerializableSort;
import org.polypheny.db.algebra.logical.LogicalTableScan.SerializableTableScan;
import org.polypheny.db.algebra.logical.LogicalUnion.SerializableUnion;
import org.polypheny.db.algebra.logical.LogicalValues.SerializableValues;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.AlgRecordType.AlgRecordTypeSerializer;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCall.RexCallSerializer;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexDynamicParam.RexDynamicParamSerializer;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexInputRef.RexInputRefSerializer;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLiteral.RexLiteralSerializer;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexSubQuery.RexSubQuerySerializer;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.ArrayType.ArrayTypeSerializer;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.BasicPolyType.BasicPolyTypeSerializer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Collation.CollationSerializer;
import org.polypheny.db.util.SerializableCharset;
import org.polypheny.db.util.SerializableCharset.SerializableCharsetSerializer;

public class PolySerializer {

    public static final FSTConfiguration conf = null;//FSTConfiguration.createDefaultConfiguration();

    public static final Kryo kryo = new Kryo();


    static {

        /*conf.registerClass(
                SerializableTableScan.class,
                SerializableJoin.class,
                SerializableAggregate.class,
                SerializableFilter.class,
                SerializableJoin.class,
                SerializableSort.class,
                SerializableProject.class,
                SerializableUnion.class,
                SerializableValues.class );*/

        kryo.setRegistrationRequired( false );
        kryo.setWarnUnregisteredClasses( true );

        // algebra
        kryo.register( SerializableJoin.class );
        kryo.register( SerializableProject.class );
        kryo.register( SerializableFilter.class );
        kryo.register( SerializableValues.class );
        kryo.register( SerializableUnion.class );
        kryo.register( SerializableAggregate.class );
        kryo.register( SerializableSort.class );
        kryo.register( SerializableTableScan.class );

        // type
        kryo.register( BasicPolyType.class, new BasicPolyTypeSerializer() );
        kryo.register( Collation.class, new CollationSerializer() );
        kryo.register( SerializableCharset.class, new SerializableCharsetSerializer() );
        kryo.register( AlgRecordType.class, new AlgRecordTypeSerializer() );
        kryo.register( JoinAlgType.class );
        kryo.register( PolyType.class );
        kryo.register( ArrayType.class, new ArrayTypeSerializer() );
        kryo.register( AlgFieldCollation.class, new AlgFieldCollationSerializer() );
        kryo.register( AlgCollation.class, new AlgCollationSerializer() );
        kryo.register( AlgCollationImpl.class, new AlgCollationSerializer() );

        // rex
        kryo.register( RexLiteral.class, new RexLiteralSerializer() );
        kryo.register( RexSubQuery.class, new RexSubQuerySerializer( null ) );
        kryo.register( RexInputRef.class, new RexInputRefSerializer() );
        kryo.register( RexCall.class, new RexCallSerializer() );
        kryo.register( RexInputRef.class, new RexInputRefSerializer() );
        kryo.register( RexDynamicParam.class, new RexDynamicParamSerializer() );

        // utility
        ImmutableListSerializer immutableListSerializer = new ImmutableListSerializer();
        kryo.register( ImmutableList.class, immutableListSerializer );
        kryo.register( ImmutableList.of().getClass(), immutableListSerializer );
        kryo.register( ImmutableList.of( "-" ).getClass(), immutableListSerializer );
        kryo.register( ArrayList.class );
        kryo.register( List.class );
    }


    public static byte[] serializeAndCompress( Object object ) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeflaterOutputStream deflate = new DeflaterOutputStream( out );

        Output output = new Output( deflate );
        kryo.writeObject( output, object );

        output.flush();
        output.close();
        return out.toByteArray();
    }


    public static <T> T deserializeAndCompress( byte[] in, Class<T> clazz ) {
        ByteArrayInputStream inp = new ByteArrayInputStream( in );
        InflaterInputStream inflate = new InflaterInputStream( inp );

        Input input = new Input( inflate );

        return kryo.readObject( input, clazz );
    }


    public static byte[] compress( byte[] in ) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream deflate = new DeflaterOutputStream( out );
            deflate.write( in );
            deflate.flush();
            deflate.close();

            return out.toByteArray();
        } catch ( IOException e ) {
            throw new RuntimeException( "The compression of the received byte array was not successful." );
        }
    }


    public static byte[] decompress( byte[] in ) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream inflate = new InflaterOutputStream( out );
            inflate.write( in );
            inflate.flush();
            inflate.close();

            return out.toByteArray();
        } catch ( IOException e ) {
            throw new RuntimeException( "The decompression of the received byte array was not successful." );
        }
    }


    public static byte[] asByteArray( Object object ) {
        return conf.asByteArray( object );
    }


    public static byte[] asCompressedByteArray( Object object ) {
        return compress( asByteArray( object ) );
    }


    public static <T> T asObject( byte[] array, Class<T> clazz ) {
        return clazz.cast( conf.asObject( array ) );
    }


    public static <T> T asDecompressedObject( byte[] array, Class<T> clazz ) {
        return asObject( decompress( array ), clazz );
    }


    public static <T> T asDecompressedObject( String array, Class<T> clazz ) {
        return asDecompressedObject( ByteString.parseBase64( array ), clazz );
    }


    public static String asCompressedByteString( Object node ) {
        return new ByteString( asCompressedByteArray( node ) ).toBase64String();
    }


    public static class ImmutableListSerializer extends Serializer<ImmutableList<?>> {

        @Override
        public void write( Kryo kryo, Output output, ImmutableList<?> object ) {
            kryo.writeClassAndObject( output, Lists.newArrayList( object ) );
        }


        @Override
        public ImmutableList<?> read( Kryo kryo, Input input, Class<? extends ImmutableList<?>> type ) {
            Builder<?> builder = ImmutableList.builder();
            builder
                    .addAll( (List) kryo.readClassAndObject( input ) )
                    .build();

            return builder.build();
        }

    }

}
