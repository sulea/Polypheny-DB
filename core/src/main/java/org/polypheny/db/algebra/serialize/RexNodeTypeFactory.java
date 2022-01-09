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

package org.polypheny.db.algebra.serialize;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.BasicPolyType;

@SuppressWarnings("unchecked")
public class RexNodeTypeFactory implements TypeAdapterFactory {

    final static Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory( new AlgNodeTypeFactory() )
            .registerTypeAdapterFactory( new OptTypeFactory() )
            .registerTypeAdapterFactory( new RexNodeTypeFactory() )
            .serializeNulls()
            .create();


    @Override
    public <T> TypeAdapter<T> create( Gson gson, TypeToken<T> type ) {
        if ( RexCall.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new RexCallTypeAdapter();
        }
        return null;
    }


    public static class RexCallTypeAdapter extends TypeAdapter<RexCall> {

        @Override
        public void write( JsonWriter out, RexCall value ) throws IOException {
            out.beginObject();
            out.name( "op" );
            gson.toJson( value.op, Operator.class, out );
            out.name( "operands" );
            gson.toJson( value.operands, ImmutableList.class, out );
            out.name( "type" );
            gson.toJson( value.type, AlgDataType.class, out );
            out.endObject();
        }


        @Override
        public RexCall read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            Operator op = gson.fromJson( in, Operator.class );
            in.nextName();
            ImmutableList<RexNode> ops = gson.fromJson( in, ImmutableList.class );
            in.nextName();
            AlgDataType type = gson.fromJson( in, BasicPolyType.class );
            in.endObject();
            return new RexCall( type, op, ops );
        }

    }

}
