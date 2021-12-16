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

package org.polypheny.db.processing;

import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.LogicalConditionalTableModify;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.transaction.Statement;

public class EnumerableTableModifyAdjuster {

    public static AlgRoot adjust( AlgRoot root, Statement statement ) {
        return AlgRoot.of( LogicalConditionalTableModify.create( (LogicalTableModify) root.alg, statement ), Kind.UPDATE );
        /*List<AlgRoot> updates = new ArrayList<>();
        LogicalTableModify modify = ((LogicalTableModify) root.alg);
        Filter filter = (Filter) modify.getInput();

        // add all previous variables e.g. _id, _data(previous), _data(updated)
        // might only extract previous refs used in condition e.g. _data
        List<String> update = new ArrayList<>( filter.getRowType().getFieldNames() );
        List<RexNode> source = filter.getRowType().getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), filter.getRowType() ) ).collect( Collectors.toList() );

        update.addAll( modify.getUpdateColumnList() );
        source.addAll( modify.getSourceExpressionList() );

        Project project = LogicalProject.create( modify.getInput(), source, update );
        AlgRoot updated = AlgRoot.of( project, Kind.SELECT );

        ProposedImplementations implementations = prepareQueryList( updated, updated.alg.getRowType(), isRouted, isSubQuery );

        PolyResult result = implementations.getResults().get( 0 );
        List<List<Object>> rows = result.getRows( statement, -1 );
        for ( List<Object> row : rows ) {
            assert row.size() == 3;
            AlgBuilder builder = AlgBuilder.create( statement );
            // push TableScan
            builder.scan( modify.getTable().getQualifiedName() );
            builder = builder.filter(
                    builder.getRexBuilder().makeCall(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            builder.getRexBuilder().makeLiteral(
                                    row.get( 0 ),
                                    updated.validatedRowType.getFieldList().get( 0 ).getType(),
                                    true ),
                            builder.getRexBuilder().makeInputRef( updated.validatedRowType, 0 ) ) );

            AlgDataType type = updated.validatedRowType.getFieldList().get( 1 ).getType();

            updates.add( AlgRoot.of( LogicalTableModify.create(
                    modify.getTable(),
                    modify.getCatalogReader(),
                    builder.build(),
                    Operation.UPDATE,
                    Collections.singletonList( updated.validatedRowType.getFieldNames().get( 1 ) ),
                    Collections.singletonList(
                            row.get( 2 ) instanceof String
                                    ? builder.getRexBuilder().makeLiteral( (String) row.get( 2 ) )
                                    : builder.getRexBuilder().makeLiteral(
                                            row.get( 2 ),
                                            type, true ) ),
                    false ), Kind.UPDATE ) );
        }

        for ( AlgRoot algRoot : updates ) {
            prepareQuery( algRoot, algRoot.validatedRowType, false, true, false ).getRows( statement, -1 );
        }
        AlgBuilder builder = AlgBuilder.create( statement );

        AlgRoot newRoot = AlgRoot.of(
                builder.scan( indexLookupRoot.alg.getTable().getQualifiedName() ).aggregate( builder.groupKey(), builder.countStar( "ROWCOUNT" ) ).build(), Kind.SELECT );

        proposedRoutingPlans = route( newRoot, statement, logicalQueryInformation );*/
    }

}
