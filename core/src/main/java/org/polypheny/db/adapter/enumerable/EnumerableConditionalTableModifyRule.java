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

package org.polypheny.db.adapter.enumerable;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.LogicalConditionalTableModify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;

public class EnumerableConditionalTableModifyRule extends ConverterRule {

    public EnumerableConditionalTableModifyRule() {
        super( LogicalConditionalTableModify.class,
                operand -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER, "EnumerableConditionalTableModify" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalConditionalTableModify lce = (LogicalConditionalTableModify) alg;
        final AlgNode input = AlgOptRule.convert( lce.getModify(), lce.getModify().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final AlgNode query = AlgOptRule.convert( lce.getQuery(), lce.getQuery().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final AlgNode prepared = AlgOptRule.convert( lce.getPrepared(), lce.getPrepared().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return EnumerableConditionalTableModify.create( input, query, prepared );
    }

}
