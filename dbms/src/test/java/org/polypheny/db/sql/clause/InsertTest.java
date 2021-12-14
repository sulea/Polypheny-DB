/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.sql.clause;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class InsertTest {

    private static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS insert_test (id INTEGER NOT NULL, PRIMARY KEY (id))";

    private static final String CREATE_TABLE_MULTIPLE =
            "CREATE TABLE IF NOT EXISTS insert_test (id INTEGER NOT NULL, cid INTEGER NOT NULL, PRIMARY KEY (id))";


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @AfterClass
    public static void stop() throws SQLException {
    }


    private void enableConstraints() throws SQLException {
        TestHelper.getInstance();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( "ALTER CONFIG 'runtime/uniqueConstraintEnforcement' SET true" );
                //statement.executeUpdate( "ALTER CONFIG 'runtime/foreignKeyEnforcement' SET true" );
                //statement.executeUpdate( "ALTER CONFIG 'runtime/polystoreIndexesSimplify' SET true" );
            }
        }
    }


    @Test
    public void chainedSingleInsertTest() throws SQLException {
        enableConstraints();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE );

                try {
                    statement.executeUpdate( "INSERT INTO insert_test VALUES (1), (2)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(*) FROM insert_test" ),
                            ImmutableList.of( new Object[]{ 2L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE insert_test" );
                }
            }


        }
    }


    @Test
    public void chainedMultipleInsertTest() throws SQLException {
        enableConstraints();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_MULTIPLE );

                try {
                    statement.executeUpdate( "INSERT INTO insert_test VALUES (1,2), (2,3)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(*) FROM insert_test" ),
                            ImmutableList.of( new Object[]{ 2L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE insert_test" );
                }
            }


        }
    }

}
