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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;

@SuppressWarnings("unchecked")
public class OptTypeFactory implements TypeAdapterFactory {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory( new AlgNodeTypeFactory() )
            .registerTypeAdapterFactory( new OptTypeFactory() )
            .registerTypeAdapterFactory( new RexNodeTypeFactory() )
            .serializeNulls()
            .create();

    static final private AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );


    @Override
    public <T> TypeAdapter<T> create( Gson gson, TypeToken<T> type ) {
        if ( AlgOptTable.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new AlgTableOptTypeAdapter().nullSafe();
        }
        if ( AlgOptSchema.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) getGenericTypeAdapter( AlgOptSchema.class, PolyphenyDbCatalogReader.class );
        }
        if ( AlgRecordType.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new AlgRecordTypeAdapter().nullSafe();
        }
        if ( BasicPolyType.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new BasicPolyTypeAdapter().nullSafe();
        }
        if ( AlgDataTypeField.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new AlgDataTypeFieldAdapter().nullSafe();
        }
        if ( AlgDataType.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new AlgRecordTypeAdapter().nullSafe();
        }

        return null;
    }


    public static class AlgTableOptTypeAdapter extends TypeAdapter<AlgOptTable> {

        @Override
        public void write( JsonWriter out, AlgOptTable value ) throws IOException {
            out.beginObject();
            out.name( "schema" );
            gson.toJson( value.getRelOptSchema(), AlgOptSchema.class, out );
            out.name( "rowType" );
            gson.toJson( value.getRowType(), AlgRecordType.class, out );
            out.name( "names" );
            gson.toJson( value.getQualifiedName(), List.class, out );
            out.endObject();
        }


        @Override
        public AlgOptTable read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            AlgOptSchema schema = gson.fromJson( in, AlgOptSchema.class );
            in.nextName();
            AlgDataType rowType = gson.fromJson( in, AlgRecordType.class );
            in.nextName();
            List<String> names = gson.fromJson( in, List.class );
            in.endObject();
            return AlgOptTableImpl.create( schema, rowType, names, null );
        }

    }


    public static class BasicPolyTypeAdapter extends TypeAdapter<BasicPolyType> {


        @Override
        public void write( JsonWriter out, BasicPolyType value ) throws IOException {
            out.beginObject();
            out.name( "typeName" );
            gson.toJson( value.getPolyType(), PolyType.class, out );
            out.name( "nullable" );
            out.value( value.isNullable() );
            out.name( "precision" );
            out.value( value.getPrecision() );
            out.name( "scale" );
            out.value( value.getScale() );
            out.endObject();
        }


        @Override
        public BasicPolyType read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            PolyType type = gson.fromJson( in, PolyType.class );
            in.nextName();
            boolean nullable = in.nextBoolean();
            in.nextName();
            int precision = in.nextInt();
            in.nextName();
            int scale = in.nextInt();

            in.endObject();
            if ( type.allowsPrec() && type.allowsScale() ) {
                return new BasicPolyType( typeFactory.getTypeSystem(), type, precision, scale ).createWithNullability( nullable );
            }

            if ( type.allowsPrec() ) {
                return new BasicPolyType( typeFactory.getTypeSystem(), type, precision ).createWithNullability( nullable );
            }

            return new BasicPolyType( typeFactory.getTypeSystem(), type ).createWithNullability( nullable );
        }

    }


    public static class AlgRecordTypeAdapter extends TypeAdapter<AlgRecordType> {

        @Override
        public void write( JsonWriter out, AlgRecordType value ) throws IOException {
            out.beginObject();
            out.name( "fields" );
            out.beginArray();
            for ( AlgDataTypeField typeField : value.getFieldList() ) {
                gson.toJson( typeField, AlgDataTypeField.class, out );
            }
            out.endArray();
            out.endObject();
        }


        @Override
        public AlgRecordType read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            in.beginArray();
            List<AlgDataTypeField> fields = new ArrayList<>();
            while ( in.peek() != JsonToken.END_ARRAY ) {
                fields.add( gson.fromJson( in, AlgDataTypeField.class ) );
            }
            in.endArray();
            in.endObject();
            return new AlgRecordType( fields );
        }

    }


    public static class AlgDataTypeFieldAdapter extends TypeAdapter<AlgDataTypeField> {

        @Override
        public void write( JsonWriter out, AlgDataTypeField value ) throws IOException {
            out.beginObject();
            out.name( "name" );
            out.value( value.getName() );
            out.name( "physical" );
            out.value( value.getPhysicalName() );
            out.name( "index" );
            out.value( value.getIndex() );
            out.name( "type" );
            gson.toJson( value.getType(), BasicPolyType.class, out );
            out.endObject();
        }


        @Override
        public AlgDataTypeField read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            String name = in.nextString();
            in.nextName();
            String physical;
            if ( in.peek() == JsonToken.NULL ) {
                in.nextNull();
                physical = null;
            } else {
                physical = in.nextString();
            }
            in.nextName();
            int index = in.nextInt();
            in.nextName();
            AlgDataType type = gson.fromJson( in, BasicPolyType.class );
            in.endObject();
            return new AlgDataTypeFieldImpl( name, physical, index, type );
        }

    }


    public <T, E> TypeAdapter<T> getGenericTypeAdapter( Class<T> iFace, Class<E> actual ) {
        return new TypeAdapter<T>() {
            @Override
            public void write( JsonWriter out, T value ) {
                new Gson().toJson( value, iFace, out );
            }


            @Override
            public T read( JsonReader in ) {
                return new Gson().fromJson( in, actual );
            }
        }.nullSafe();
    }

}
