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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class JoinTest {


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
        addComplexTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA joinTest" );
                statement.executeUpdate( "CREATE TABLE TableA(ID VARCHAR(255) NOT NULL, NAME VARCHAR(255), AMOUNT INTEGER, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Ab', 'Name1', 10000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Bc', 'Name2',  5000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Cd', 'Name3',  7000.00)" );

                statement.executeUpdate( "CREATE TABLE TableB(ID VARCHAR(255) NOT NULL, SALARY INTEGER, IDB VARCHAR(255) NOT NULL, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableB VALUES ('Ab', 400.00, 'Ab')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES ('Cd',  20.00, 'Ab')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES ('De',  7.00, 'De')" );

                statement.executeUpdate( "CREATE TABLE TableC(AMOUNT INTEGER NOT NULL, PRIMARY KEY (AMOUNT))" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (10000)" );

                statement.executeUpdate( "CREATE TABLE joinTest.Table_C(AMOUNT INTEGER NOT NULL, PRIMARY KEY (AMOUNT))" );
                statement.executeUpdate( "INSERT INTO joinTest.Table_C VALUES (10000)" );

                connection.commit();
            }
        }
    }


    private static void addComplexTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA cineast" );

                statement.executeUpdate( "CREATE TABLE cineast.features_averagecolor (id INTEGER NOT NULL, feature DOUBLE ARRAY(1,3), PRIMARY KEY (id))" );
                statement.executeUpdate( "CREATE TABLE cineast.cineast_segment (id INTEGER NOT NULL, segmentid INTEGER, objectid INTEGER, PRIMARY KEY (id))" );
                statement.executeUpdate( "CREATE TABLE cineast.cineast_multimediaobject (objectid INTEGER NOT NULL, PRIMARY KEY (objectid))" );

                statement.executeUpdate( "INSERT INTO cineast.features_averagecolor VALUES (1, ARRAY[0, 0, 0])" );
                statement.executeUpdate( "INSERT INTO cineast.cineast_segment VALUES (1, 1, 1)" );
                statement.executeUpdate( "INSERT INTO cineast.cineast_multimediaobject VALUES (1)" );
                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTestData( statement );
                dropComplexData( statement );
            }
            connection.commit();
        }
    }


    private static void dropTestData( Statement statement ) throws SQLException {
        statement.executeUpdate( "DROP TABLE TableA" );
        statement.executeUpdate( "DROP TABLE TableB" );
        statement.executeUpdate( "DROP TABLE TableC" );
        statement.executeUpdate( "DROP TABLE joinTest.Table_C" );
        statement.executeUpdate( "DROP SCHEMA joinTest" );
    }


    private static void dropComplexData( Statement statement ) throws SQLException {
        statement.executeUpdate( "DROP TABLE cineast.features_averagecolor" );
        statement.executeUpdate( "DROP TABLE cineast.cineast_segment" );
        statement.executeUpdate( "DROP TABLE cineast.cineast_multimediaobject" );
        statement.executeUpdate( "DROP SCHEMA cineast" );
    }

    // --------------- Tests ---------------


    @Test
    public void naturalJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Name1", "Ab", 10000 },
                        new Object[]{ "Name2", "Bc", 5000 },
                        new Object[]{ "Name3", "Cd", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S NATURAL JOIN (SELECT name, Amount  FROM TableA) AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void naturalTwoTableJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, 400, "Ab" },
                        new Object[]{ "Cd", "Name3", 7000, 20, "Ab" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM TableA NATURAL JOIN (SELECT *  FROM TableB)" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void twoNaturalJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab", 10000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM TableA, TableB, TableC WHERE TableA.Amount = TableC.Amount AND TableB.ID = TableA.ID" ),
                        expectedResult,
                        true );
            }
        }
    }

    @Test
    public void twoNaturalJoinUnderscoreTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab", 10000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM TableA, TableB, joinTest.Table_C WHERE TableA.Amount = joinTest.Table_C.Amount AND TableB.ID = TableA.ID" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void twoNaturalJoinMixSchemaTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab", 10000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM TableA, TableB, joinTest.Table_C WHERE TableA.Amount = joinTest.Table_C.Amount AND TableB.ID = TableA.ID" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void innerJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S INNER JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void innerTwoTableJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab" },
                        new Object[]{ "Ab", "Name1", 10000, "Cd", 20, "Ab" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM ( SELECT * FROM TableA) AS A INNER JOIN (SELECT * FROM TableB) AS B ON A.id = B.idb" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void leftJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S LEFT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void leftTwoJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab" },
                        new Object[]{ "Bc", "Name2", 5000, null, null, null },
                        new Object[]{ "Cd", "Name3", 7000, "Cd", 20, "Ab" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT * FROM TableA) AS S LEFT JOIN (SELECT *  FROM TableB) AS T ON S.id = T.id" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    // todo dl, rewrite this test
    public void rightJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S RIGHT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void rightTwoJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", 10000, "Ab", 400, "Ab" },
                        new Object[]{ "Cd", "Name3", 7000, "Cd", 20, "Ab" },
                        new Object[]{ null, null, null, "De", 7, "De" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT * FROM TableA) AS S RIGHT JOIN (SELECT * FROM TableB) AS T ON S.id = T.id" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void fullJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S FULL JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void complexJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.374165748, 1, 1, 1, 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM "
                                + "(SELECT id, distance(feature, ARRAY[0.1,0.2,0.3], 'L2') as dist "
                                + "FROM cineast.features_averagecolor ORDER BY dist ASC LIMIT 500) AS feature "
                                + "INNER JOIN cineast.cineast_segment AS segment ON (feature.id = segment.segmentid) "
                                + "INNER JOIN cineast.cineast_multimediaobject AS object ON (segment.objectid = object.objectid)" ),
                        expectedResult,
                        true );
            }
        }
    }

}
