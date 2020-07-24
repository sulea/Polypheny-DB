/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.jdbc.stores;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.jdbc.JdbcSchema;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public abstract class AbstractJdbcStore extends Store {

    private InformationPage informationPage;
    private InformationGroup informationGroupConnectionPool;
    private List<Information> informationElements;

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;


    public AbstractJdbcStore(
            int storeId,
            String uniqueName,
            Map<String, String> settings,
            ConnectionFactory connectionFactory,
            SqlDialect dialect,
            boolean persistent ) {
        super( storeId, uniqueName, settings, false, false, persistent );
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        // Register the JDBC Pool Size as information in the information manager
        registerJdbcPoolSizeInformation( uniqueName );
    }


    protected void registerJdbcPoolSizeInformation( String uniqueName ) {
        informationPage = new InformationPage( uniqueName ).setLabel( "Stores" );
        informationGroupConnectionPool = new InformationGroup( informationPage, "JDBC Connection Pool" );

        informationElements = new ArrayList<>();

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupConnectionPool );

        InformationGraph connectionPoolSizeGraph = new InformationGraph(
                informationGroupConnectionPool,
                GraphType.DOUGHNUT,
                new String[]{ "Active", "Available", "Idle" }
        );
        im.registerInformation( connectionPoolSizeGraph );
        informationElements.add( connectionPoolSizeGraph );

        InformationTable connectionPoolSizeTable = new InformationTable(
                informationGroupConnectionPool,
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( connectionPoolSizeTable );
        informationElements.add( connectionPoolSizeTable );

        informationGroupConnectionPool.setRefreshFunction( () -> {
            int idle = connectionFactory.getNumIdle();
            int active = connectionFactory.getNumActive();
            int max = connectionFactory.getMaxTotal();
            int available = max - idle - active;

            connectionPoolSizeGraph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( getUniqueName() + "-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            connectionPoolSizeTable.reset();
            connectionPoolSizeTable.addRow( "Active", active );
            connectionPoolSizeTable.addRow( "Idle", idle );
            connectionPoolSizeTable.addRow( "Max", max );
        } );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        //return new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), new JdbcConvention( DatabaseProduct.HSQLDB.getDialect(), expression, "myjdbcconvention" ), "testdb", null, combinedSchema );
        // TODO MV: Potential bug! This only works as long as we do not cache the schema between multiple transactions
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, connectionFactory, dialect, this );
    }


    protected abstract String getTypeString( PolyType polyType );


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogTable.getSchemaName() );
        qualifiedNames.add( catalogTable.name );
        String physicalTableName = getPhysicalTableName( catalogTable.id );
        if ( log.isDebugEnabled() ) {
            log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), qualifiedNames, physicalTableName );
        }
        StringBuilder query = buildCreateTableQuery( getDefaultPhysicalSchemaName(), physicalTableName, catalogTable );
        executeUpdate( query, context );
        // Add physical names to placements
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnStore( getStoreId(), catalogTable.id ) ) {
            try {
                catalog.updateColumnPlacementPhysicalNames(
                        getStoreId(),
                        placement.columnId,
                        getDefaultPhysicalSchemaName(),
                        physicalTableName,
                        getPhysicalColumnName( placement.columnId ) );
            } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
                throw new RuntimeException( e );
            }
        }

    }


    protected StringBuilder buildCreateTableQuery( String schemaName, String physicalTableName, CatalogTable catalogTable ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE TABLE " )
                .append( dialect.quoteIdentifier( schemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) )
                .append( " ( " );
        boolean first = true;
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnStore( getStoreId(), catalogTable.id ) ) {
            CatalogColumn catalogColumn;
            try {
                catalogColumn = catalog.getColumn( placement.columnId );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                throw new RuntimeException( e );
            }
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( getPhysicalColumnName( placement.columnId ) ) ).append( " " );

            if ( !this.dialect.supportsNestedArrays() && catalogColumn.collectionsType != null ) {
                //returns e.g. TEXT if arrays are not supported
                builder.append( getTypeString( PolyType.ARRAY ) );
            } else {
                builder.append( getTypeString( catalogColumn.type ) );
                if ( catalogColumn.length != null ) {
                    builder.append( "(" ).append( catalogColumn.length );
                    if ( catalogColumn.scale != null ) {
                        builder.append( "," ).append( catalogColumn.scale );
                    }
                    builder.append( ")" );
                }
                if ( catalogColumn.collectionsType != null ) {
                    builder.append( " " ).append( catalogColumn.collectionsType.toString() );
                    //TODO NH check if can apply dimension / cardinality
                    /*if ( catalogColumn.dimension != null ) {
                        builder.append( "(" ).append( catalogColumn.dimension );
                        if ( catalogColumn.cardinality != null ) {
                            builder.append( "," ).append( catalogColumn.cardinality );
                        }
                        builder.append( ")" );
                    }*/
                }
            }

        }
        builder.append( " )" );
        return builder;
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        String physicalColumnName = getPhysicalColumnName( catalogColumn.id );
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows adding columns to linked tables.
        String physicalTableName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalTableName;
        String physicalSchemaName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalSchemaName;
        StringBuilder query = buildAddColumnQuery( physicalSchemaName, physicalTableName, physicalColumnName, catalogTable, catalogColumn );
        executeUpdate( query, context );
        // Insert default value
        if ( catalogColumn.defaultValue != null ) {
            query = buildInsertDefaultValueQuery( physicalSchemaName, physicalTableName, physicalColumnName, catalogColumn.defaultValue );
            executeUpdate( query, context );
        }
        // Add physical name to placement
        try {
            catalog.updateColumnPlacementPhysicalNames(
                    getStoreId(),
                    catalogColumn.id,
                    physicalSchemaName,
                    physicalTableName,
                    physicalColumnName );
        } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
            throw new RuntimeException( e );
        }
    }


    protected StringBuilder buildAddColumnQuery( String physicalSchemaName, String physicalTableName, String physicalColumnName, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " ADD " ).append( dialect.quoteIdentifier( physicalColumnName ) ).append( " " );
        if ( !this.dialect.supportsNestedArrays() && catalogColumn.collectionsType != null ) {
            //returns e.g. TEXT if arrays are not supported
            builder.append( getTypeString( PolyType.ARRAY ) );
        } else {
            builder.append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.length != null ) {
                builder.append( "(" ).append( catalogColumn.length );
                if ( catalogColumn.scale != null ) {
                    builder.append( "," ).append( catalogColumn.scale );
                }
                builder.append( ")" );
            }
            if ( catalogColumn.collectionsType != null ) {
                builder.append( " " ).append( catalogColumn.collectionsType.toString() );
            }
        }
        builder.append( " NULL" );
        return builder;
    }


    protected StringBuilder buildInsertDefaultValueQuery( String physicalSchemaName, String physicalTableName, String physicalColumnName, CatalogDefaultValue defaultValue ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " SET " ).append( dialect.quoteIdentifier( physicalColumnName ) ).append( " = " );
        SqlLiteral literal;
        switch ( defaultValue.type ) {
            case BOOLEAN:
                literal = SqlLiteral.createBoolean( Boolean.parseBoolean( defaultValue.value ), SqlParserPos.ZERO );
                break;
            case INTEGER:
            case DECIMAL:
            case BIGINT:
                literal = SqlLiteral.createExactNumeric( defaultValue.value, SqlParserPos.ZERO );
                break;
            case REAL:
            case DOUBLE:
                literal = SqlLiteral.createApproxNumeric( defaultValue.value, SqlParserPos.ZERO );
                break;
            case VARCHAR:
                literal = SqlLiteral.createCharString( defaultValue.value, SqlParserPos.ZERO );
                break;
            default:
                throw new PolyphenyDbException( "Not yet supported default value type: " + defaultValue.type );
        }
        builder.append( literal.toSqlString( dialect ) );
        return builder;
    }


    // Make sure to update overriden methods as well
    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
        builder.append( " " ).append( getTypeString( catalogColumn.type ) );
        if ( catalogColumn.length != null ) {
            builder.append( "(" );
            builder.append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
        executeUpdate( builder, context );
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows dropping linked tables.
        String physicalTableName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalTableName;
        String physicalSchemaName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalSchemaName;
        StringBuilder builder = new StringBuilder();
        builder.append( "DROP TABLE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder, context );
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
        executeUpdate( builder, context );
    }


    @Override
    public void truncate( Context context, CatalogTable catalogTable ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows truncating linked tables.
        String physicalTableName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalTableName;
        String physicalSchemaName = Catalog.getInstance().getColumnPlacementsOnStore( getStoreId(), catalogTable.id ).get( 0 ).physicalSchemaName;
        StringBuilder builder = new StringBuilder();
        builder.append( "TRUNCATE TABLE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder, context );
    }


    protected void executeUpdate( StringBuilder builder, Context context ) {
        try {
            context.getTransaction().registerInvolvedStore( this );
            connectionFactory.getOrCreateConnectionHandler( context.getTransaction().getXid() ).executeUpdate( builder.toString() );
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new RuntimeException( e );
        }
    }


    @SneakyThrows
    @Override
    public boolean prepare( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            return connectionFactory.getConnectionHandler( xid ).prepare();
        } else {
            log.warn( "There is no connection to prepare (Uniquename: {}, XID: {})! Returning true.", getUniqueName(), xid );
            return true;
        }
    }


    @SneakyThrows
    @Override
    public void commit( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).commit();
        } else {
            log.warn( "There is no connection to commit (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    @SneakyThrows
    @Override
    public void rollback( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).rollback();
        } else {
            log.warn( "There is no connection to rollback (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    protected String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    protected String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }


    protected abstract String getDefaultPhysicalSchemaName();


    protected void removeInformationPage() {
        if ( informationElements.size() > 0 ) {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationElements.toArray( new Information[0] ) );
            im.removeGroup( informationGroupConnectionPool );
            im.removePage( informationPage );
        }
    }

}