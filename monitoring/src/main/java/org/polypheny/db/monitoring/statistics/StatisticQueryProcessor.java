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

package org.polypheny.db.monitoring.statistics;


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;


@Slf4j
public class StatisticQueryProcessor {

    @Getter
    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    /**
     * LowCostQueries can be used to retrieve short answered queries
     * Idea is to expose a selected list of sql operations with a small list of results and not impact performance
     */
    public StatisticQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
    }


    public StatisticQueryProcessor( TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, "pa", "APP" );
    }


    /**
     * Handles the request for one columns stats
     *
     * @return result of the query
     */
    public StatisticQueryColumn selectOneColumnStat( AlgNode node, Transaction transaction, Statement statement, QueryColumn queryColumn ) {
        StatisticResult res = this.executeColStat( node, transaction, statement, queryColumn );
        if ( res.getColumns() != null && res.getColumns().length == 1 ) {
            return res.getColumns()[0];
        }
        return null;
    }


    public String selectTableStat( AlgNode node, Transaction transaction, Statement statement ) {
        return this.executeOneTableStat( node, transaction, statement );
    }


    /**
     * Method to get all schemas, tables, and their columns in a database
     */
    public List<List<String>> getSchemaTree() {
        Catalog catalog = Catalog.getInstance();
        List<List<String>> result = new ArrayList<>();
        List<String> schemaTree = new ArrayList<>();
        List<CatalogSchema> schemas = catalog.getSchemas( new Pattern( databaseName ), null );
        for ( CatalogSchema schema : schemas ) {
            List<String> tables = new ArrayList<>();
            List<CatalogTable> childTables = catalog.getTables( schema.id, null );
            for ( CatalogTable childTable : childTables ) {
                List<String> table = new ArrayList<>();
                List<CatalogColumn> childColumns = catalog.getColumns( childTable.id );
                for ( CatalogColumn catalogColumn : childColumns ) {
                    table.add( schema.name + "." + childTable.name + "." + catalogColumn.name );
                }
                if ( childTable.tableType == TableType.TABLE ) {
                    tables.addAll( table );
                }
            }
            schemaTree.addAll( tables );
            result.add( schemaTree );
        }
        return result;
    }


    /**
     * Gets all columns in the database
     *
     * @return all the columns
     */
    public List<QueryColumn> getAllColumns() {
        Catalog catalog = Catalog.getInstance();
        List<CatalogColumn> catalogColumns = catalog.getColumns(
                new Pattern( databaseName ),
                null,
                null,
                null );
        List<QueryColumn> allColumns = new ArrayList<>();

        for ( CatalogColumn catalogColumn : catalogColumns ) {
            if ( catalog.getTable( catalogColumn.tableId ).tableType != TableType.VIEW ) {
                allColumns.add( new QueryColumn( catalogColumn.schemaId, catalogColumn.tableId, catalogColumn.id, catalogColumn.type ) );
            }
        }
        return allColumns;
    }


    /**
     * Gets all tables in the database
     *
     * @return all the tables ids
     */
    public List<CatalogTable> getAllTable() {
        Catalog catalog = Catalog.getInstance();
        List<CatalogTable> catalogTables = catalog.getTables(
                new Pattern( databaseName ),
                null,
                null );
        List<CatalogTable> allTables = new ArrayList<>();

        for ( CatalogTable catalogTable : catalogTables ) {
            if ( catalogTable.tableType != TableType.VIEW ) {
                allTables.add( catalogTable );
            }
        }
        return allTables;
    }


    /**
     * Get all columns of a specific table
     *
     * @return all columns
     */
    public List<QueryColumn> getAllColumns( Long tableId ) {
        Catalog catalog = Catalog.getInstance();
        List<QueryColumn> columns = new ArrayList<>();
        catalog.getColumns( tableId ).forEach( c -> columns.add( QueryColumn.fromCatalogColumn( c ) ) );
        return columns;
    }


    private StatisticResult executeColStat( AlgNode node, Transaction transaction, Statement statement, QueryColumn queryColumn ) {
        StatisticResult result = new StatisticResult();
        try {
            result = executeColStat( statement, node, queryColumn );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    private String executeOneTableStat( AlgNode node, Transaction transaction, Statement statement ) {
        String result = "";
        try {
            result = executeOneTableStat( statement, node );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false, "Statistics", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private StatisticResult executeColStat( Statement statement, AlgNode node, QueryColumn queryColumn ) throws QueryExecutionException {
        PolyResult result;
        List<List<Object>> rows;

        try {
            result = statement.getQueryProcessor().prepareQuery( AlgRoot.of( node, Kind.SELECT ), node.getRowType(), true );
            rows = result.getRows( statement, getPageSize() );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        List<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    temp[counter] = o.toString();
                }
                counter++;
            }
            data.add( temp );
        }

        String[][] d = data.toArray( new String[0][] );

        return new StatisticResult( queryColumn, d );
    }


    private String executeOneTableStat( Statement statement, AlgNode node ) throws QueryExecutionException {
        PolyResult result;
        List<List<Object>> rows;

        try {
            result = statement.getQueryProcessor().prepareQuery( AlgRoot.of( node, Kind.SELECT ), node.getRowType(), true );
            rows = result.getRows( statement, getPageSize() );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        List<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    temp[counter] = o.toString();
                }
                counter++;
            }
            data.add( temp );
        }

        String[][] d = data.toArray( new String[0][] );

        return d[0][0];
    }


    /**
     * Get the page
     */
    private int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    public static String buildQualifiedName( String... strings ) {
        return "\"" + String.join( "\".\"", strings ) + "\"";
    }


    static class QueryExecutionException extends Exception {

        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }

}