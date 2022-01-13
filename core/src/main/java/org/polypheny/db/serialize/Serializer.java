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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;
import org.nustaq.serialization.FSTConfiguration;

public class Serializer {

    public static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();


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

}
