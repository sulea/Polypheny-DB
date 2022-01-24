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

package org.polypheny.db.algebra.rules;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;

@Slf4j
public class OptimizedJoinRule extends AlgOptRule {

    public static final OptimizedJoinRule INSTANCE = new OptimizedJoinRule();


    public OptimizedJoinRule() {
        super( operandJ( LogicalJoin.class, null, r -> !r.isOptimized(), operand( AlgNode.class, any() ), operand( AlgNode.class, any() ) ), AlgFactories.LOGICAL_BUILDER, "OptimizedJoinRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        log.warn( String.valueOf( call ) );
        LogicalJoin join = call.alg( 0 );

        AlgNode left = call.alg( 1 );
        AlgNode right = call.alg( 2 );
        AlgBuilder builder = call.builder();
        RexBuilder rexBuilder = builder.getRexBuilder();

        builder.push( left );
        AlgDataType elementType = builder.getTypeFactory().createPolyType( PolyType.VARCHAR, 255 );
        AlgDataType arrayType = builder.getTypeFactory().createArrayType( elementType, -1 );
        RexNode cond = builder.inArray(
                rexBuilder.makeInputRef( elementType, 0 ),
                rexBuilder.makeDynamicParam( arrayType, 2 ) );
        builder.filter( cond );

        left = builder.build();

        join = join.copy( join.getTraitSet(), join.getCondition(), left, right, join.getJoinType(), join.isSemiJoinDone() );
        join.setOptimized( true );

        call.transformTo( join );
    }

}
