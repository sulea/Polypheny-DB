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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConditionalTableModify;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

public class EnumerableAdjuster {

    // todo dl use shuttle to get all included TableModifies...
    public static AlgRoot adjustModify( AlgRoot root, Statement statement ) {
        if ( root.alg instanceof TableModify ) {
            return AlgRoot.of( LogicalConditionalTableModify.create( (LogicalTableModify) root.alg, statement ), Kind.UPDATE );
        } else {
            ModifyAdjuster adjuster = new ModifyAdjuster( statement );
            root.alg.accept( adjuster );
            return root;
        }
    }


    public static AlgRoot adjustBatch( AlgRoot root, Statement statement ) {
        return AlgRoot.of( LogicalBatchIterator.create( root.alg, statement ), root.kind );
    }


    public static boolean needsAdjustment( AlgNode alg ) {
        if ( alg instanceof TableModify ) {
            return ((TableModify) alg).isUpdate();
        }

        boolean needsAdjustment = false;
        for ( AlgNode input : alg.getInputs() ) {
            needsAdjustment |= needsAdjustment( input );
        }
        return needsAdjustment;
    }


    public static AlgRoot adjustConstraint( AlgRoot root, Statement statement ) {
        return AlgRoot.of(
                LogicalConstraintEnforcer.create( root.alg, statement ),
                root.kind );
    }


    public static AlgRoot prerouteJoins( AlgRoot root, Statement statement, QueryProcessor queryProcessor ) {
        JoinAdjuster adjuster = new JoinAdjuster( statement, queryProcessor );
        return AlgRoot.of( root.alg.accept( adjuster ), root.kind );
    }


    private static class ModifyAdjuster extends AlgShuttleImpl {

        private final Statement statement;


        private ModifyAdjuster( Statement statement ) {
            this.statement = statement;
        }


        @Override
        public AlgNode visit( LogicalConditionalExecute lce ) {
            if ( lce.getRight() instanceof TableModify ) {
                AlgNode ctm = LogicalConditionalTableModify.create( (LogicalTableModify) lce.getRight(), statement );
                lce.replaceInput( 1, ctm );
                return lce;
            } else {
                return lce.getRight().accept( this );
            }
        }

    }


    private static class JoinAdjuster extends AlgShuttleImpl {

        private final Statement statement;
        private final QueryProcessor queryProcessor;


        public JoinAdjuster( Statement statement, QueryProcessor queryProcessor ) {
            this.statement = statement;
            this.queryProcessor = queryProcessor;
        }


        @Override
        // todo dl, rewrite extremely prototypy
        public AlgNode visit( LogicalJoin join ) {
            AlgBuilder builder = AlgBuilder.create( statement );
            RexBuilder rexBuilder = builder.getRexBuilder();
            AlgNode left = join.getLeft().accept( this );
            AlgNode right = join.getRight().accept( this );

            List<RexNode> operands = ((RexCall) join.getCondition()).operands;
            // extract underlying right operators which compare left to right

            return preRouteOneSide( join, builder, rexBuilder, left, right, operands );
        }


        private AlgNode preRouteOneSide( LogicalJoin join, AlgBuilder builder, RexBuilder rexBuilder, AlgNode left, AlgNode right, List<RexNode> operands ) {
            boolean preRouteRight = join.getJoinType() != JoinAlgType.LEFT;
            int ordinal = ((RexInputRef) operands.get( 1 )).getIndex() - left.getRowType().getFieldCount();
            if ( !preRouteRight ) {
                ordinal = ((RexInputRef) operands.get( 0 )).getIndex();
            }
            AlgNode projectedRight = builder
                    .push( preRouteRight ? right : left )
                    .project( builder.field( ordinal ) )
                    .build();

            PolyResult result = queryProcessor.prepareQuery( AlgRoot.of( projectedRight, Kind.SELECT ), false );
            List<List<Object>> rows = result.getRows( statement, -1 );

            builder.push( preRouteRight ? left : right );

            List<RexNode> nodes = new ArrayList<>();
            RexNode leftComp = operands.get( preRouteRight ? 0 : 1 );

            for ( List<Object> row : rows ) {
                List<RexNode> ands = new ArrayList<>();
                for ( Object o : row ) {
                    int index = ((RexInputRef) leftComp).getIndex();
                    if ( !preRouteRight ) {
                        index = ((RexInputRef) operands.get( 1 )).getIndex() - left.getRowType().getFieldCount();
                    }
                    ands.add(
                            rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.EQUALS ),
                                    builder.field( index ),
                                    rexBuilder.makeLiteral( o, leftComp.getType(), false ) ) );
                }
                if ( ands.size() > 1 ) {
                    nodes.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), ands ) );
                } else {
                    nodes.add( ands.get( 0 ) );
                }
            }
            AlgNode prepared;
            if ( nodes.size() > 1 ) {
                prepared = builder.filter( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), nodes ) ).build();
            } else {
                prepared = builder.filter( nodes.get( 0 ) ).build();
            }
            builder.push( preRouteRight ? prepared : left );
            builder.push( preRouteRight ? right : prepared );
            builder.join( join.getJoinType(), join.getCondition() );
            return builder.build();
        }

    }

}
