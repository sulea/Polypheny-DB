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
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;

@SuppressWarnings("unchecked")
public class AlgNodeTypeFactory implements TypeAdapterFactory {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory( new AlgNodeTypeFactory() )
            .registerTypeAdapterFactory( new OptTypeFactory() )
            .registerTypeAdapterFactory( new RexNodeTypeFactory() )
            .serializeNulls()
            .create();

    public static AlgOptCluster cluster;


    @Override
    public <T> TypeAdapter<T> create( Gson gson, TypeToken<T> type ) {
        if ( TableScan.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new TableScanTypeAdapter().nullSafe();
        }

        if ( Filter.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new FilterTypeAdapter().nullSafe();
        }

        if ( Operator.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new OperatorTypeAdapter().nullSafe();
        }

        if ( ImmutableList.class.isAssignableFrom( type.getRawType() ) ) {
            return (TypeAdapter<T>) new ImmutableListTypeAdapter().nullSafe();
        }

        return null;
    }


    public static class FilterTypeAdapter extends TypeAdapter<Filter> {

        @Override
        public void write( JsonWriter out, Filter value ) throws IOException {
            out.beginObject();
            out.name( "condition" );
            gson.toJson( value.getCondition(), RexCall.class, out );
            out.name( "input" );
            gson.toJson( value.getInput(), AlgNode.class, out );
            out.name( "variableSet" );
            gson.toJson( value.getVariablesSet(), List.class, out );
            out.endObject();
        }


        @Override
        public Filter read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            RexNode condition = gson.fromJson( in, RexCall.class );
            in.nextName();
            AlgNode input = gson.fromJson( in, AlgNode.class );
            in.nextName();
            Collection<CorrelationId> vars = gson.fromJson( in, List.class );
            ImmutableSet<CorrelationId> variables = ImmutableSet.copyOf( vars );
            in.endObject();
            return LogicalFilter.create( input, condition, variables );
        }

    }


    public static class TableScanTypeAdapter extends TypeAdapter<TableScan> {

        @Override
        public void write( JsonWriter out, TableScan scan ) throws IOException {
            out.beginObject();
            out.name( "table" );
            gson.toJson( scan.getTable(), AlgOptTable.class, out );
            out.endObject();
        }


        @Override
        public TableScan read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            AlgOptTable table = gson.fromJson( in, AlgOptTable.class );
            in.endObject();
            return LogicalTableScan.create( cluster, table );
        }

    }


    public static class OperatorTypeAdapter extends TypeAdapter<Operator> {

        @Override
        public void write( JsonWriter out, Operator value ) throws IOException {
            out.beginObject();
            out.name( "name" );
            out.value( value.getOperatorName().name() );
            out.endObject();
        }


        @Override
        public Operator read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            String name = in.nextString();
            in.endObject();
            return OperatorRegistry.get( OperatorName.valueOf( name ) );
        }

    }


    public static class ImmutableListTypeAdapter extends TypeAdapter<ImmutableList> {

        @Override
        public void write( JsonWriter out, ImmutableList value ) throws IOException {
            out.beginObject();
            out.name( "values" );
            gson.toJson( value.asList(), List.class, out );
            out.endObject();
        }


        @Override
        public ImmutableList read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            List list = gson.fromJson( in, List.class );
            in.endObject();
            return ImmutableList.copyOf( list );
        }

    }

}
