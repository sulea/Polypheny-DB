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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.lpg.LogicalGraph;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.*;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.webui.models.Result;

import static org.polypheny.db.ddl.DdlManagerImpl.getResult;


@Slf4j
public class DataMigratorImpl implements DataMigrator {

    @Override
    public void copyGraphData( CatalogGraphDatabase target, Transaction transaction, Integer existingAdapterId, CatalogAdapter to ) {
        Statement statement = transaction.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        LogicalLpgScan scan = (LogicalLpgScan) builder.lpgScan( target.id ).build();

        AlgNode routed = RoutingManager.getInstance().getFallbackRouter().handleGraphScan( scan, statement, existingAdapterId );

        AlgRoot algRoot = AlgRoot.of( routed, Kind.SELECT );

        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg::getCluster,
                true );
        algRoot = algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );

        PolyImplementation result = statement.getQueryProcessor().prepareQuery(
                algRoot,
                algRoot.alg.getCluster().getTypeFactory().builder().build(),
                true,
                false,
                false );

        final Enumerable<Object> enumerable = result.enumerable( statement.getDataContext() );

        Iterator<Object> sourceIterator = enumerable.iterator();

        if ( sourceIterator.hasNext() ) {
            PolyGraph graph = (PolyGraph) sourceIterator.next();

            if ( graph.getEdges().isEmpty() && graph.getNodes().isEmpty() ) {
                // nothing to copy
                return;
            }

            // we have a new statement
            statement = transaction.createStatement();
            builder = AlgBuilder.create( statement );

            LogicalLpgValues values = getLogicalLpgValues( builder, graph );

            LogicalLpgModify modify = new LogicalLpgModify( builder.getCluster(), builder.getCluster().traitSetOf( ModelTrait.GRAPH ), new LogicalGraph( target.id ), values, Operation.INSERT, null, null );

            AlgNode routedModify = RoutingManager.getInstance().getDmlRouter().routeGraphDml( modify, statement, target, List.of( to.id ) );

            result = statement.getQueryProcessor().prepareQuery(
                    AlgRoot.of( routedModify, Kind.SELECT ),
                    routedModify.getCluster().getTypeFactory().builder().build(),
                    true,
                    false,
                    false );

            final Enumerable<Object> modifyEnumerable = result.enumerable( statement.getDataContext() );

            Iterator<Object> modifyIterator = modifyEnumerable.iterator();
            if ( modifyIterator.hasNext() ) {
                modifyIterator.next();
            }

        }


    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void copyRelationalDataToDocumentData( Transaction transaction, CatalogTable sourceTable, long targetSchemaId ) {
        try {
            Catalog catalog = Catalog.getInstance();

            // Collect the columns of the source table
            List<CatalogColumn> sourceColumns = new ArrayList<>();
            for ( String columnName : sourceTable.getColumnNames() ) {
                sourceColumns.add( catalog.getColumn( sourceTable.id, columnName ) );
            }

            // Retrieve the placements of the source table
            Map<Long, List<CatalogColumnPlacement>> sourceColumnPlacements = new HashMap<>();
            sourceColumnPlacements.put(
                    sourceTable.partitionProperty.partitionIds.get( 0 ),
                    selectSourcePlacements( sourceTable, sourceColumns, -1 ) );
            Map<Long, List<CatalogColumnPlacement>> subDistribution = new HashMap<>( sourceColumnPlacements );
            subDistribution.keySet().retainAll( Arrays.asList( sourceTable.partitionProperty.partitionIds.get( 0 ) ) );

            // Initialize the source statement to read all values from the source table
            Statement sourceStatement = transaction.createStatement();
            AlgRoot sourceAlg = getSourceIterator( sourceStatement, subDistribution );
            PolyImplementation result = sourceStatement.getQueryProcessor().prepareQuery(
                    sourceAlg,
                    sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                    true,
                    false,
                    false );

            // Build the data structure to map the columns to the physical placements
            Map<String, Integer> sourceColMapping = new LinkedHashMap<>();
            for ( CatalogColumn catalogColumn : sourceColumns ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        sourceColMapping.put( catalogColumn.name, i );
                    }
                    i++;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            Iterator<Object> sourceIterator = enumerable.iterator();
            while ( sourceIterator.hasNext() ) {
                // Build a data structure for all values of the source table for the insert query
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
                List<LinkedHashMap<String, Object>> values = new ArrayList<>();
                for ( List<Object> list : rows ) {
                    LinkedHashMap<String, Object> currentRowValues = new LinkedHashMap<>();
                    sourceColMapping.forEach( ( key, value ) -> currentRowValues.put( key, list.get( value ) ) );
                    values.add( currentRowValues );
                }

                // Create the insert query for all documents in the collection
                boolean firstRow = true;
                StringBuffer bf = new StringBuffer();
                bf.append( "db." + sourceTable.name + ".insertMany([" );
                for ( Map<String, Object> row : values ) {
                    if ( firstRow ) {
                        bf.append( "{" );
                        firstRow = false;
                    } else {
                        bf.append( ",{" );
                    }
                    boolean firstColumn = true;
                    for ( Map.Entry<String, Object> entry : row.entrySet() ) {
                        if ( entry.getValue() != null ) {
                            if ( firstColumn == true ) {
                                firstColumn = false;
                            } else {
                                bf.append( "," );
                            }
                            bf.append( "\"" + entry.getKey() + "\" : \"" + entry.getValue() + "\"" );
                        }
                    }
                    bf.append( "}" );
                }
                bf.append( "])" );

                // Insert als documents into the newlz created collection
                Statement targetStatement = transaction.createStatement();
                String query = bf.toString();
                AutomaticDdlProcessor mqlProcessor = (AutomaticDdlProcessor) transaction.getProcessor( Catalog.QueryLanguage.MONGO_QL );
                QueryParameters parameters = new MqlQueryParameters( query, catalog.getSchema( targetSchemaId ).name, Catalog.NamespaceType.DOCUMENT );
                MqlNode parsed = (MqlNode) mqlProcessor.parse( query ).get( 0 );
                AlgRoot logicalRoot = mqlProcessor.translate( targetStatement, parsed, parameters );
                PolyImplementation polyImplementation = targetStatement.getQueryProcessor().prepareQuery( logicalRoot, true );

                // TODO: something is wrong with the transactions. Try to get rid of this.
                Result updateRresult = getResult( Catalog.QueryLanguage.MONGO_QL, targetStatement, query, polyImplementation, transaction, false );
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void copyDocumentDataToRelationalData( Transaction transaction, List<JsonObject> jsonObjects, CatalogTable targetTable ) throws UnknownColumnException {
        Catalog catalog = Catalog.getInstance();

        // Get the values in all documents of the collection
        // TODO: A data structure is needed to represent also 1:N relations of multiple tables
        Map<CatalogColumn, List<Object>> columnValues = new HashMap<>();
        for ( JsonObject jsonObject : jsonObjects ) {
            for ( String columnName : targetTable.getColumnNames() ) {
                CatalogColumn column = catalog.getColumn( targetTable.id, columnName );
                if ( !columnValues.containsKey( column ) ) {
                    columnValues.put( column, new LinkedList<>() );
                }
                JsonElement jsonElement = jsonObject.get( columnName );
                if ( jsonElement != null ) {
                    columnValues.get( column ).add( jsonElement.getAsString() );
                } else {
                    columnValues.get( column ).add( null );
                }
            }
        }

        Statement targetStatement = transaction.createStatement();
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( Entry<CatalogColumn, List<Object>> entry : columnValues.entrySet() ) {
            // Add the values to the column to the statement
            CatalogColumn targetColumn = catalog.getColumn( targetTable.id, entry.getKey().name );
            targetStatement.getDataContext().addParameterValues( targetColumn.id, targetColumn.getAlgDataType( typeFactory ), entry.getValue() );

            // Add all placements of the column to the targetColumnPlacements list
            for ( DataStore store : RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewColumn( targetColumn ) ) {
                CatalogColumnPlacement columnPlacement = Catalog.getInstance().getColumnPlacement( store.getAdapterId(), targetColumn.id );
                targetColumnPlacements.add( columnPlacement );
            }
        }

        // Prepare the insert query
        AlgRoot targetAlg = buildInsertStatement( targetStatement, targetColumnPlacements, targetTable.partitionProperty.partitionIds.get( 0 ) );
        Iterator<?> iterator = targetStatement.getQueryProcessor()
                .prepareQuery( targetAlg, targetAlg.validatedRowType, true, false, false )
                .enumerable( targetStatement.getDataContext() )
                .iterator();
        //noinspection WhileLoopReplaceableByForEach
        while ( iterator.hasNext() ) {
            iterator.next();
        }

        targetStatement.getDataContext().resetParameterValues();
    }


    @NotNull
    private static LogicalLpgValues getLogicalLpgValues( AlgBuilder builder, PolyGraph graph ) {
        List<AlgDataTypeField> fields = new ArrayList<>();
        int index = 0;
        if ( !graph.getNodes().isEmpty() ) {
            fields.add( new AlgDataTypeFieldImpl( "n", index, builder.getTypeFactory().createPolyType( PolyType.NODE ) ) );
            index++;
        }
        if ( !graph.getEdges().isEmpty() ) {
            fields.add( new AlgDataTypeFieldImpl( "e", index, builder.getTypeFactory().createPolyType( PolyType.EDGE ) ) );
        }

        return new LogicalLpgValues( builder.getCluster(), builder.getCluster().traitSetOf( ModelTrait.GRAPH ), graph.getNodes().values(), graph.getEdges().values(), ImmutableList.of(), new AlgRecordType( fields ) );
    }


    @Override
    public void copyData( Transaction transaction, CatalogAdapter store, List<CatalogColumn> columns, List<Long> partitionIds ) {
        CatalogTable table = Catalog.getInstance().getTable( columns.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( table.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        // We need a columnPlacement for every partition
        Map<Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();
        if ( table.partitionProperty.isPartitioned ) {
            PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
            PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( table.partitionProperty.partitionType );
            placementDistribution = partitionManager.getRelevantPlacements( table, partitionIds, Collections.singletonList( store.id ) );
        } else {
            placementDistribution.put(
                    table.partitionProperty.partitionIds.get( 0 ),
                    selectSourcePlacements( table, selectColumnList, targetColumnPlacements.get( 0 ).adapterId ) );
        }

        for ( long partitionId : partitionIds ) {
            Statement sourceStatement = transaction.createStatement();
            Statement targetStatement = transaction.createStatement();

            Map<Long, List<CatalogColumnPlacement>> subDistribution = new HashMap<>( placementDistribution );
            subDistribution.keySet().retainAll( List.of( partitionId ) );
            AlgRoot sourceAlg = getSourceIterator( sourceStatement, subDistribution );
            AlgRoot targetAlg;
            if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, table.id ).size() == columns.size() ) {
                // There have been no placements for this table on this store before. Build insert statement
                targetAlg = buildInsertStatement( targetStatement, targetColumnPlacements, partitionId );
            } else {
                // Build update statement
                targetAlg = buildUpdateStatement( targetStatement, targetColumnPlacements, partitionId );
            }

            // Execute Query
            executeQuery( selectColumnList, sourceAlg, sourceStatement, targetStatement, targetAlg, false, false );
        }
    }


    @Override
    public void executeQuery( List<CatalogColumn> selectColumnList, AlgRoot sourceAlg, Statement sourceStatement, Statement targetStatement, AlgRoot targetAlg, boolean isMaterializedView, boolean doesSubstituteOrderBy ) {
        try {
            PolyImplementation result;
            if ( isMaterializedView ) {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        false,
                        false,
                        doesSubstituteOrderBy );
            } else {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        true,
                        false,
                        false );
            }
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }
            if ( isMaterializedView ) {
                for ( CatalogColumn catalogColumn : selectColumnList ) {
                    if ( !resultColMapping.containsKey( catalogColumn.id ) ) {
                        int i = resultColMapping.values().stream().mapToInt( v -> v ).max().orElseThrow( NoSuchElementException::new );
                        resultColMapping.put( catalogColumn.id, i + 1 );
                    }
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            int i = 0;
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
                Map<Long, List<Object>> values = new HashMap<>();

                for ( List<Object> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        if ( isMaterializedView ) {
                            if ( entry.getValue() > list.size() - 1 ) {
                                values.get( entry.getKey() ).add( i );
                                i++;
                            } else {
                                values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                            }
                        } else {
                            values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                        }
                    }
                }
                List<AlgDataTypeField> fields;
                if ( isMaterializedView ) {
                    fields = targetAlg.alg.getTable().getRowType().getFieldList();
                } else {
                    fields = sourceAlg.validatedRowType.getFieldList();
                }
                int pos = 0;
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues(
                            v.getKey(),
                            fields.get( resultColMapping.get( v.getKey() ) ).getType(),
                            v.getValue() );
                    pos++;
                }

                Iterator<?> iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetAlg, sourceAlg.validatedRowType, true, false, false )
                        .enumerable( targetStatement.getDataContext() )
                        .iterator();
                //noinspection WhileLoopReplaceableByForEach
                while ( iterator.hasNext() ) {
                    iterator.next();
                }
                targetStatement.getDataContext().resetParameterValues();
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    @Override
    public void executeMergeQuery( List<CatalogColumn> primaryKeyColumns, List<CatalogColumn> sourceColumns, CatalogColumn targetColumn, String joinString, AlgRoot sourceAlg, Statement sourceStatement, Statement targetStatement, AlgRoot targetAlg, boolean isMaterializedView, boolean doesSubstituteOrderBy ) {
        try {
            PolyImplementation result;
            if ( isMaterializedView ) {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        false,
                        false,
                        doesSubstituteOrderBy );
            } else {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        true,
                        false,
                        false );
            }
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            // Get the mappings of the source columns from the Catalog
            Map<Long, Integer> sourceColMapping = new LinkedHashMap<>();
            for ( CatalogColumn catalogColumn : sourceColumns ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        sourceColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            if ( isMaterializedView ) {
                for ( CatalogColumn catalogColumn : sourceColumns ) {
                    if ( !sourceColMapping.containsKey( catalogColumn.id ) ) {
                        int i = sourceColMapping.values().stream().mapToInt( v -> v ).max().orElseThrow( NoSuchElementException::new );
                        sourceColMapping.put( catalogColumn.id, i + 1 );
                    }
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            int i = 0;
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
                Map<Long, List<Object>> values = new LinkedHashMap<>();

                // Read the values of the source columns from all rows
                for ( List<Object> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : sourceColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        if ( isMaterializedView ) {
                            if ( entry.getValue() > list.size() - 1 ) {
                                values.get( entry.getKey() ).add( i );
                                i++;
                            } else {
                                values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                            }
                        } else {
                            values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                        }
                    }
                }

                // Combine the source values into a single string
                final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
                List<Object> mergedValueList = null;
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    if ( !primaryKeyColumns.stream().map( c -> c.id ).collect( Collectors.toList() ).contains( v.getKey() ) ) {
                        if ( mergedValueList == null ) {
                            mergedValueList = v.getValue();
                        } else {
                            int j = 0;
                            for ( Object value : mergedValueList ) {
                                mergedValueList.set( j, ((String) value).concat( joinString + v.getValue().get( j++ ) ) );
                            }
                        }
                    }
                }
                targetStatement.getDataContext().addParameterValues( targetColumn.id, targetColumn.getAlgDataType( typeFactory ), mergedValueList );

                // Select the PK columns for the target statement
                for ( CatalogColumn primaryKey : primaryKeyColumns ) {
                    AlgDataType primaryKeyAlgDataType = primaryKey.getAlgDataType( typeFactory );
                    List<Object> primaryKeyValues = values.get( primaryKey.id );
                    targetStatement.getDataContext().addParameterValues( primaryKey.id, primaryKeyAlgDataType, primaryKeyValues );
                }

                Iterator<?> iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetAlg, sourceAlg.validatedRowType, true, false, false )
                        .enumerable( targetStatement.getDataContext() )
                        .iterator();
                //noinspection WhileLoopReplaceableByForEach
                while ( iterator.hasNext() ) {
                    iterator.next();
                }
                targetStatement.getDataContext().resetParameterValues();
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    @Override
    public AlgRoot buildDeleteStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.DELETE,
                null,
                null,
                true
        );

        return AlgRoot.of( node, Kind.DELETE );
    }


    @Override
    public AlgRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        // while adapters should be able to handle unsorted columnIds for prepared indexes,
        // this often leads to errors, and can be prevented by sorting
        List<CatalogColumnPlacement> placements = to.stream().sorted( Comparator.comparingLong( p -> p.columnId ) ).collect( Collectors.toList() );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : placements ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.INSERT,
                null,
                null,
                true
        );
        return AlgRoot.of( node, Kind.INSERT );
    }


    private AlgRoot buildUpdateStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.scan( qualifiedTableName );

        // build condition
        RexNode condition = null;
        CatalogTable catalogTable = Catalog.getInstance().getTable( to.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumnPlacement ccp = Catalog.getInstance().getColumnPlacement( to.get( 0 ).adapterId, cid );
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            RexNode c = builder.equals(
                    builder.field( ccp.getLogicalColumnName() ),
                    new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id )
            );
            if ( condition == null ) {
                condition = c;
            } else {
                condition = builder.and( condition, c );
            }
        }
        builder = builder.filter( condition );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }

        builder.projectPlus( values );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.UPDATE,
                columnNames,
                values,
                false
        );
        AlgRoot algRoot = AlgRoot.of( node, Kind.UPDATE );
        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg::getCluster,
                true );
        return algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );
    }


    @Override
    public AlgRoot getSourceIterator( Statement statement, Map<Long, List<CatalogColumnPlacement>> placementDistribution ) {

        // Build Query
        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );

        AlgNode node = RoutingManager.getInstance().getFallbackRouter().buildJoinedScan( statement, cluster, placementDistribution );
        return AlgRoot.of( node, Kind.SELECT );
    }


    public static List<CatalogColumnPlacement> selectSourcePlacements( CatalogTable table, List<CatalogColumn> columns, int excludingAdapterId ) {
        // Find the adapter with the most column placements
        Catalog catalog = Catalog.getInstance();
        int adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : catalog.getColumnPlacementsByAdapter( table.id ).entrySet() ) {
            if ( entry.getKey() != excludingAdapterId && entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        List<Long> columnIds = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            columnIds.add( catalogColumn.id );
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : table.fieldIds ) {
            if ( columnIds.contains( cid ) ) {
                if ( catalog.getDataPlacement( adapterIdWithMostPlacements, table.id ).columnPlacementsOnAdapter.contains( cid ) ) {
                    placementList.add( catalog.getColumnPlacement( adapterIdWithMostPlacements, cid ) );
                } else {
                    for ( CatalogColumnPlacement placement : catalog.getColumnPlacement( cid ) ) {
                        if ( placement.adapterId != excludingAdapterId ) {
                            placementList.add( placement );
                            break;
                        }
                    }
                }
            }
        }

        return placementList;
    }


    /**
     * Currently used to to transfer data if partitioned table is about to be merged.
     * For Table Partitioning use {@link #copyPartitionData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, List, List)}  } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Source Table from where data is queried
     * @param columns Necessary columns on target
     * @param placementDistribution Pre-computed mapping of partitions and the necessary column placements
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    @Override
    public void copySelectiveData( Transaction transaction, CatalogAdapter store, CatalogTable sourceTable, CatalogTable targetTable, List<CatalogColumn> columns, Map<Long, List<CatalogColumnPlacement>> placementDistribution, List<Long> targetPartitionIds ) {
        CatalogPrimaryKey sourcePrimaryKey = Catalog.getInstance().getPrimaryKey( sourceTable.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : sourcePrimaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();

        AlgRoot sourceAlg = getSourceIterator( sourceStatement, placementDistribution );
        AlgRoot targetAlg;
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, targetTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetAlg = buildInsertStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        } else {
            // Build update statement
            targetAlg = buildUpdateStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        }

        // Execute Query
        try {
            PolyImplementation result = sourceStatement.getQueryProcessor().prepareQuery( sourceAlg, sourceAlg.alg.getCluster().getTypeFactory().builder().build(), true, false, false );
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect(
                        result.getCursorFactory(),
                        LimitIterator.of( sourceIterator, batchSize ),
                        new ArrayList<>() );
                Map<Long, List<Object>> values = new HashMap<>();
                for ( List<Object> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                    }
                }
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues( v.getKey(), null, v.getValue() );
                }
                Iterator<?> iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetAlg, sourceAlg.validatedRowType, true, false, true )
                        .enumerable( targetStatement.getDataContext() )
                        .iterator();

                //noinspection WhileLoopReplaceableByForEach
                while ( iterator.hasNext() ) {
                    iterator.next();
                }
                targetStatement.getDataContext().resetParameterValues();
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    /**
     * Currently used to transfer data if unpartitioned is about to be partitioned.
     * For Table Merge use {@link #copySelectiveData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, Map, List)}   } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Target Table where data is to be inserted
     * @param columns Necessary columns on target
     * @param sourcePartitionIds Source Partitions which need to be considered for querying
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    @Override
    public void copyPartitionData( Transaction transaction, CatalogAdapter store, CatalogTable sourceTable, CatalogTable targetTable, List<CatalogColumn> columns, List<Long> sourcePartitionIds, List<Long> targetPartitionIds ) {
        if ( sourceTable.id != targetTable.id ) {
            throw new RuntimeException( "Unsupported migration scenario. Table ID mismatch" );
        }

        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( sourceTable.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        // Add partition columns to select column list
        long partitionColumnId = targetTable.partitionProperty.partitionColumnId;
        CatalogColumn partitionColumn = Catalog.getInstance().getColumn( partitionColumnId );
        if ( !selectColumnList.contains( partitionColumn ) ) {
            selectColumnList.add( partitionColumn );
        }

        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( targetTable.partitionProperty.partitionType );

        //We need a columnPlacement for every partition
        Map<Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();

        placementDistribution.put( sourceTable.partitionProperty.partitionIds.get( 0 ), selectSourcePlacements( sourceTable, selectColumnList, -1 ) );

        Statement sourceStatement = transaction.createStatement();

        //Map PartitionId to TargetStatementQueue
        Map<Long, Statement> targetStatements = new HashMap<>();

        //Creates queue of target Statements depending
        targetPartitionIds.forEach( id -> targetStatements.put( id, transaction.createStatement() ) );

        Map<Long, AlgRoot> targetAlgs = new HashMap<>();

        AlgRoot sourceAlg = getSourceIterator( sourceStatement, placementDistribution );
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, sourceTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetPartitionIds.forEach( id -> targetAlgs.put( id, buildInsertStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        } else {
            // Build update statement
            targetPartitionIds.forEach( id -> targetAlgs.put( id, buildUpdateStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        }

        // Execute Query
        try {
            PolyImplementation result = sourceStatement.getQueryProcessor().prepareQuery( sourceAlg, sourceAlg.alg.getCluster().getTypeFactory().builder().build(), true, false, false );
            final Enumerable<?> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = (Iterator<Object>) enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            int partitionColumnIndex = -1;
            String parsedValue = null;
            String nullifiedPartitionValue = partitionManager.getUnifiedNullValue();
            if ( targetTable.partitionProperty.isPartitioned ) {
                if ( resultColMapping.containsKey( targetTable.partitionProperty.partitionColumnId ) ) {
                    partitionColumnIndex = resultColMapping.get( targetTable.partitionProperty.partitionColumnId );
                } else {
                    parsedValue = nullifiedPartitionValue;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );

                Map<Long, Map<Long, List<Object>>> partitionValues = new HashMap<>();

                for ( List<Object> row : rows ) {
                    long currentPartitionId = -1;
                    if ( partitionColumnIndex >= 0 ) {
                        parsedValue = nullifiedPartitionValue;
                        if ( row.get( partitionColumnIndex ) != null ) {
                            parsedValue = row.get( partitionColumnIndex ).toString();
                        }
                    }

                    currentPartitionId = partitionManager.getTargetPartitionId( targetTable, parsedValue );

                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( entry.getKey() == partitionColumn.id && !columns.contains( partitionColumn ) ) {
                            continue;
                        }
                        if ( !partitionValues.containsKey( currentPartitionId ) ) {
                            partitionValues.put( currentPartitionId, new HashMap<>() );
                        }
                        if ( !partitionValues.get( currentPartitionId ).containsKey( entry.getKey() ) ) {
                            partitionValues.get( currentPartitionId ).put( entry.getKey(), new LinkedList<>() );
                        }
                        partitionValues.get( currentPartitionId ).get( entry.getKey() ).add( row.get( entry.getValue() ) );
                    }
                }

                // Iterate over partitionValues in that way we don't even execute a statement which has no rows
                for ( Map.Entry<Long, Map<Long, List<Object>>> dataOnPartition : partitionValues.entrySet() ) {
                    long partitionId = dataOnPartition.getKey();
                    Map<Long, List<Object>> values = dataOnPartition.getValue();
                    Statement currentTargetStatement = targetStatements.get( partitionId );

                    for ( Map.Entry<Long, List<Object>> columnDataOnPartition : values.entrySet() ) {
                        // Check partitionValue
                        currentTargetStatement.getDataContext().addParameterValues( columnDataOnPartition.getKey(), null, columnDataOnPartition.getValue() );
                    }

                    Iterator iterator = currentTargetStatement.getQueryProcessor()
                            .prepareQuery( targetAlgs.get( partitionId ), sourceAlg.validatedRowType, true, false, false )
                            .enumerable( currentTargetStatement.getDataContext() )
                            .iterator();
                    //noinspection WhileLoopReplaceableByForEach
                    while ( iterator.hasNext() ) {
                        iterator.next();
                    }
                    currentTargetStatement.getDataContext().resetParameterValues();
                }
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    @Override
    public void mergeColumns( Transaction transaction, CatalogAdapter store, List<CatalogColumn> sourceColumns, CatalogColumn targetColumn, String joinString ) {
        CatalogTable table = Catalog.getInstance().getTable( sourceColumns.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( table.primaryKey );

        List<CatalogColumn> selectColumnList = new LinkedList<>( sourceColumns );
        List<CatalogColumn> primaryKeyList = new LinkedList<>();

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
            primaryKeyList.add( catalogColumn );
        }

        // Get the placements of the source columns
        Map<Long, List<CatalogColumnPlacement>> sourceColumnPlacements = new HashMap<>();
        sourceColumnPlacements.put(
                table.partitionProperty.partitionIds.get( 0 ),
                selectSourcePlacements( table, selectColumnList, -1 ) );

        // Get the placement of the newly added target column
        CatalogColumnPlacement targetColumnPlacement = Catalog.getInstance().getColumnPlacement( store.id, targetColumn.id );
        Map<Long, List<CatalogColumnPlacement>> subDistribution = new HashMap<>( sourceColumnPlacements );
        subDistribution.keySet().retainAll( Arrays.asList( table.partitionProperty.partitionIds.get( 0 ) ) );

        // Initialize statements for the reading and inserting
        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();
        AlgRoot sourceAlg = getSourceIterator( sourceStatement, subDistribution );
        AlgRoot targetAlg = buildUpdateStatement( targetStatement, Collections.singletonList( targetColumnPlacement ), table.partitionProperty.partitionIds.get( 0 ) );

        executeMergeQuery( primaryKeyList, selectColumnList, targetColumn, joinString, sourceAlg, sourceStatement, targetStatement, targetAlg, false, false );
    }

}
