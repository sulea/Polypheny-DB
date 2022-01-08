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

package org.polypheny.db.adapter.enumerable;

import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Streamer;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableStreamer extends Streamer implements EnumerableAlg {

    /**
     * <pre>
     *          Streamer
     *      ^               |
     *      |               v
     *  Provider        Collector
     * </pre>
     *
     * @param cluster
     * @param traitSet
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public EnumerableStreamer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();

        final Result providerResult = implementor.visitChild( this, 0, (EnumerableAlg) getProvider(), pref );
        final Result collectorResult = implementor.visitChild( this, 1, (EnumerableAlg) getCollector(), pref );

        Expression providerExp = builder.append( "provider_" + System.nanoTime(), providerResult.block );
        Expression collectorExp = builder.append( "collector_" + System.nanoTime(), collectorResult.block );

        Expression types = Expressions.constant( getCollector().getRowType().getFieldList().stream().map( f -> f.getType().getPolyType().name() ).collect( Collectors.toList() ) );

        Expression result = Expressions.call( BuiltInMethod.STREAM.method, Expressions.constant( DataContext.ROOT ), providerExp, types );
        builder.add( result );
        builder.add( Expressions.return_( null, builder.append( "collector_" + System.nanoTime(), collectorExp ) ) );

        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }

}
