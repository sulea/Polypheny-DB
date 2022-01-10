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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nustaq.serialization.FSTConfiguration;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.serialize.AlgNodeTypeFactory;
import org.polypheny.db.algebra.serialize.OptTypeFactory;
import org.polypheny.db.algebra.serialize.RexNodeTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyTypeFactoryImpl;

public class AlgNodeTypeFactoryTest {

    private static TestHelper helper = TestHelper.getInstance();
    final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
    RexBuilder rexBuilder = new RexBuilder( typeFactory );

    Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory( new AlgNodeTypeFactory() )
            .registerTypeAdapterFactory( new OptTypeFactory() )
            .registerTypeAdapterFactory( new RexNodeTypeFactory() )
            .serializeNulls()
            .create();

    private Transaction transaction;
    private AlgBuilder builder;
    static FSTConfiguration conf;


    @BeforeClass
    public static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE tableA(Id INTEGER NOT NULL,Name VARCHAR(255), Primary key(Id))" );

                connection.commit();
            }
        }
        conf = FSTConfiguration.createDefaultConfiguration();
    }


    @Before
    public void createBuilder() {
        this.transaction = helper.getTransaction();
        this.builder = AlgBuilder.create( transaction.createStatement() );
        AlgNodeTypeFactory.cluster = AlgOptCluster.create( new VolcanoPlanner(), rexBuilder );
    }


    @Test
    public void algOptTableFSTTest() {
        AlgNode scan = builder.scan( "public", "tableA" ).build();

        byte[] barray = conf.asByteArray( scan.getTable() );
        AlgOptTable table = (AlgOptTable) conf.asObject( barray );
        assert table.getRelOptSchema().equals( scan.getTable() );
    }


    @Test
    public void algOptTableTest() {
        AlgNode filter = builder.scan( "public", "tableA" ).build();

        AlgOptTable table = filter.getTable();
        String serialized = gson.toJson( table );
        AlgOptTable deserialized = gson.fromJson( serialized, AlgOptTable.class );
    }


    @Test
    public void tableScanTest() {
        AlgNode scan = builder.scan( "public", "tableA" ).build();

        byte[] barray = conf.asByteArray( scan );
        AlgNode table = (AlgNode) conf.asObject( barray );
        assert table instanceof TableScan;
    }


    @Test
    public void tableScanFSTTest() {
        AlgNode scan = builder.scan( "public", "tableA" ).build();

        String serialized = gson.toJson( scan );
        AlgNode deserialized = gson.fromJson( serialized, AlgNode.class );
        assert deserialized instanceof TableScan;
    }


    @Test
    public void testFilter() {
        AlgNode node = builder.scan( "public", "tableA" ).build();

        AlgNode filter = builder.push( node ).filter( rexBuilder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                rexBuilder.makeLiteral( "1" ),
                rexBuilder.makeInputRef( node, 0 ) ) ).build();

        String serialized = gson.toJson( filter );
        AlgNode deserialized = gson.fromJson( serialized, Filter.class );
        assert deserialized instanceof LogicalFilter;
    }

}
