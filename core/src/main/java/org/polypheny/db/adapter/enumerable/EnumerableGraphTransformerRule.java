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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.GraphTransformer;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

public class EnumerableGraphTransformerRule extends ConverterRule {

    EnumerableGraphTransformerRule() {
        super( GraphTransformer.class, r -> true, Convention.NONE, EnumerableConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, "EnumerableGraphTransformer" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        GraphTransformer graphTransformer = (GraphTransformer) alg;
        AlgTraitSet out = alg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgTraitSet inputOut = out.replace( graphTransformer.inTrait );
        return new EnumerableGraphTransformer(
                alg.getCluster(),
                out,
                alg.getInputs().stream().map( i -> convert( i, inputOut ) ).collect( Collectors.toList() ),
                alg.getRowType(),
                graphTransformer.operationOrder,
                graphTransformer.operation );
    }

}