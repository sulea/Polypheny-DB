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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.core.ConditionalTableModify;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableConditionalTableModify extends ConditionalTableModify implements EnumerableAlg {

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     * @param initialModify
     * @param query
     * @param prepared
     */
    public EnumerableConditionalTableModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode initialModify, AlgNode query, AlgNode prepared ) {
        super( cluster, traitSet, initialModify, query, prepared );
    }


    public static EnumerableConditionalTableModify create( AlgNode input, AlgNode query, AlgNode prepared ) {
        return new EnumerableConditionalTableModify( input.getCluster(), input.getTraitSet(), input, query, prepared );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableConditionalTableModify(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                inputs.get( 2 ) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        if ( getModify() instanceof ConverterImpl ) {
            return implementor.visitChild( this, 0, (EnumerableAlg) getModify(), pref );
        }

        final BlockBuilder builder = new BlockBuilder();
        final Result query = implementor.visitChild( this, 1, (EnumerableAlg) getQuery(), pref );

        MethodCallExpression transformContext = Expressions.call(
                BuiltInMethod.INTO_CONTEXT.method,
                Expressions.constant( DataContext.ROOT ),
                builder.append( builder.newName( "provider" + System.nanoTime() ), query.block ),
                Expressions.constant( getQuery().getRowType().getFieldList().stream().map( f -> f.getType().getPolyType().name() ).collect( Collectors.toList() ) ) );

        final Result prepared = implementor.visitChild( this, 2, (EnumerableAlg) getPrepared(), pref );

        builder.add( Expressions.statement( transformContext ) );
        builder.add( Expressions.return_( null, builder.append( "test", prepared.block ) ) );

        //builder.add( Expressions.return_( null, Expressions.constant( null ) ) );

        return implementor.result( prepared.physType, builder.toBlock() );
    }

}
