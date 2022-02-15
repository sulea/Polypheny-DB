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

import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.polypheny.db.adapter.enumerable.EnumerableConvention.ExternalConvention;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.mapping.ExternalTypeDefinition;

public class ExternalTypeConverter extends TypeConverter implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param input Input relational expression
     */
    protected ExternalTypeConverter( AlgOptCluster cluster, AlgNode input ) {
        super( cluster, input.getTraitSet().replace( ExternalConvention.INSTANCE ), input );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result orig = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        return EnumerableAdapterAlg.getMappingResult( implementor, pref, builder, orig, getRowType(), getConvention().getTypeDefinition(), ExternalTypeDefinition.INSTANCE );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ExternalTypeConverter( inputs.get( 0 ).getCluster(), inputs.get( 0 ) );
    }

}
