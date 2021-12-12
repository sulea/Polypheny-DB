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

import java.util.List;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Provider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;

public class LogicalProvider extends Provider {


    public LogicalProvider( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode condition, PolyResult result, Statement statement ) {
        super( cluster, traitSet, input, condition, result, statement );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return getInput().getRowType();
    }


    public static AlgNode create( Filter rel, PolyResult result, Statement statement ) {
        return new LogicalProvider( rel.getCluster(), rel.getTraitSet(), rel.getInput(), rel.getCondition(), result, statement );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).input( "input", getInput() );
    }


    @Override
    public double estimateRowCount( AlgMetadataQuery mq ) {
        return 0.6;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCost( this, mq );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalProvider( getCluster(), traitSet, getInput(), getCondition(), getResult(), getStatement() );
    }

}
