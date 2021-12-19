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

package org.polypheny.db.algebra.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.ConditionalTableModify;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

public class LogicalConditionalTableModify extends ConditionalTableModify {

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     * @param initialModify
     * @param query
     * @param prepared
     */
    public LogicalConditionalTableModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode initialModify, AlgNode query, AlgNode prepared ) {
        super( cluster, traitSet, initialModify, query, prepared );
    }


    public static LogicalConditionalTableModify create( AlgNode modify, AlgNode query, AlgNode prepared ) {
        return new LogicalConditionalTableModify( modify.getCluster(), modify.getTraitSet(), modify, query, prepared );
    }


    @Override
    public LogicalConditionalTableModify copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalConditionalTableModify(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                inputs.get( 2 ) );
    }


    public static AlgNode create( LogicalTableModify modify, Statement statement ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();
        /////// query
        // first we create the query, which could retrieve the values for the prepared modify
        // if underlying adapter cannot handle it natively
        AlgNode node = modify.getInput() instanceof Filter
                ? (Filter) modify.getInput()
                : modify.getInput().getInput( 0 ) instanceof Filter
                        ? (Filter) modify.getInput().getInput( 0 )
                        : null;
        if ( node == null ) {
            node = modify.getInput() instanceof Project ? (Project) modify.getInput() : null;
            if ( node == null ) {
                throw new RuntimeException( "The was no Filter or Project under the TableModify, which was not considered!" );
            }
        }
        // add all previous variables e.g. _id, _data(previous), _data(updated)
        // might only extract previous refs used in condition e.g. _data
        List<String> update = new ArrayList<>( getOldFieldsNames( node.getRowType().getFieldNames() ) );
        List<RexNode> source = new ArrayList<>( getOldFieldRefs( node.getRowType() ) );

        update.addAll( modify.getUpdateColumnList() );
        source.addAll( modify.getSourceExpressionList() );

        Project query = LogicalProject.create( modify.getInput(), source, update );

        /////// prepared
        List<RexNode> fields = new ArrayList<>();
        int i = 0;
        for ( AlgDataTypeField field : modify.getTable().getRowType().getFieldList() ) {
            fields.add( rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.EQUALS ),
                    rexBuilder.makeInputRef( modify.getTable().getRowType(), i ),
                    rexBuilder.makeDynamicParam( field.getType(), i ) ) );
            i++;
        }
        algBuilder.scan( modify.getTable().getQualifiedName() ).filter( fields.size() == 1
                ? fields.get( 0 )
                : rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), fields ) );
        LogicalTableModify prepared = LogicalTableModify.create(
                modify.getTable(),
                modify.getCatalogReader(),
                algBuilder.build(),
                Operation.UPDATE,
                modify.getUpdateColumnList(),
                modify.getUpdateColumnList()
                        .stream()
                        .map( name -> {
                            int size = modify.getRowType().getFieldList().size();
                            int index = modify.getTable().getRowType().getFieldNames().indexOf( name );
                            return rexBuilder.makeDynamicParam(
                                    modify.getTable().getRowType().getFieldList().get( index ).getType(), size + index );
                        } ).collect( Collectors.toList() ), false );

        return new LogicalConditionalTableModify( modify.getCluster(), modify.getTraitSet(), modify, query, prepared );
    }


    private static List<RexInputRef> getOldFieldRefs( AlgDataType rowType ) {
        return rowType.getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() );
    }


    private static List<String> getOldFieldsNames( List<String> names ) {
        return names.stream().map( name -> name + "$old" ).collect( Collectors.toList() );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
