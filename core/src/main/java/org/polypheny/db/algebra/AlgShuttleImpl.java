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

package org.polypheny.db.algebra;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.TableFunctionScan;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.graph.LogicalGraphAggregate;
import org.polypheny.db.algebra.logical.graph.LogicalGraphFilter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphMatch;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphProject;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.graph.LogicalGraphSort;
import org.polypheny.db.algebra.logical.graph.LogicalGraphUnwind;
import org.polypheny.db.algebra.logical.graph.LogicalGraphValues;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalExchange;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalMatch;
import org.polypheny.db.algebra.logical.relational.LogicalMinus;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.algebra.logical.relational.LogicalUnion;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Basic implementation of {@link AlgShuttle} that calls {@link AlgNode#accept(AlgShuttle)} on each child, and {@link AlgNode#copy(AlgTraitSet, java.util.List)} if
 * any children change.
 */
public class AlgShuttleImpl implements AlgShuttle {

    protected final Deque<AlgNode> stack = new ArrayDeque<>();


    /**
     * Visits a particular child of a parent.
     */
    protected <T extends AlgNode> T visitChild( T parent, int i, AlgNode child ) {
        stack.push( parent );
        try {
            AlgNode child2 = child.accept( this );
            if ( child2 != child ) {
                final List<AlgNode> newInputs = new ArrayList<>( parent.getInputs() );
                newInputs.set( i, child2 );
                //noinspection unchecked
                return (T) parent.copy( parent.getTraitSet(), newInputs );
            }
            return parent;
        } finally {
            stack.pop();
        }
    }


    protected <T extends AlgNode> T visitChildren( T alg ) {
        for ( Ord<AlgNode> input : Ord.zip( alg.getInputs() ) ) {
            alg = visitChild( alg, input.i, input.e );
        }
        return alg;
    }


    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( Scan scan ) {
        return scan;
    }


    @Override
    public AlgNode visit( TableFunctionScan scan ) {
        return visitChildren( scan );
    }


    @Override
    public AlgNode visit( LogicalValues values ) {
        return values;
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        return visitChild( project, 0, project.getInput() );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        return visitChildren( join );
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        return visitChildren( union );
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        return visitChildren( intersect );
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        return visitChildren( lce );
    }


    @Override
    public AlgNode visit( LogicalModify modify ) {
        return visitChildren( modify );
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        return visitChildren( enforcer );
    }


    @Override
    public AlgNode visit( LogicalGraphModify modify ) {
        return visitChildren( modify );
    }


    @Override
    public AlgNode visit( LogicalGraphScan scan ) {
        return scan;
    }


    @Override
    public AlgNode visit( LogicalGraphValues values ) {
        return values;
    }


    @Override
    public AlgNode visit( LogicalGraphFilter filter ) {
        return visitChildren( filter );
    }


    @Override
    public AlgNode visit( LogicalGraphMatch match ) {
        return visitChildren( match );
    }


    @Override
    public AlgNode visit( LogicalGraphProject project ) {
        return visitChildren( project );
    }


    @Override
    public AlgNode visit( LogicalGraphAggregate aggregate ) {
        return visitChildren( aggregate );
    }


    @Override
    public AlgNode visit( LogicalGraphSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalGraphUnwind unwind ) {
        return visitChildren( unwind );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        return visitChildren( other );
    }

}

