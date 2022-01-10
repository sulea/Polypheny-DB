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
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;
import org.nustaq.serialization.FSTConfiguration;

public class Serializer {

    public static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();


    public static byte[] compress( byte[] in ) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream defl = new DeflaterOutputStream( out );
            defl.write( in );
            defl.flush();
            defl.close();

            return out.toByteArray();
        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit( 150 );
            return null;
        }
    }


    public static byte[] decompress( byte[] in ) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream infl = new InflaterOutputStream( out );
            infl.write( in );
            infl.flush();
            infl.close();

            return out.toByteArray();
        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit( 150 );
            return null;
        }
    }

}
