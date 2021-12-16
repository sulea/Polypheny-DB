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

package org.polypheny.db.algebra.core;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
public abstract class ConditionalTableModify extends AbstractAlgNode {


    private AlgNode prepared;
    private AlgNode query;
    private AlgNode modify;


    @Override
    protected AlgDataType deriveRowType() {
        return modify.getRowType();
    }


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public ConditionalTableModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode initialModify, AlgNode query, AlgNode prepared ) {
        super( cluster, traitSet );
        this.modify = initialModify;
        this.query = query;
        this.prepared = prepared;
    }


    @Override
    public List<AlgNode> getInputs() {
        return Arrays.asList( modify, query, prepared );
    }


    @Override
    public String algCompareString() {
        return "[if " + modify.algCompareString() +
                " -> " + query.algCompareString() +
                " in " + prepared.algCompareString() + "]";
    }


    @Override
    public AlgNode getInput( int i ) {
        assert i < 3;
        return getInputs().get( i );
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( modify, 0, this );
        visitor.visit( query, 1, this );
        //visitor.visit( prepared, 2, this );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .input( "modify", modify )
                .input( "query", query )
                .input( "prepared", prepared );
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        switch ( ordinalInParent ) {
            case 0:
                this.modify = p;
                break;
            case 1:
                this.query = p;
                break;
            case 2:
                this.prepared = p;
                break;
            default:
                throw new IndexOutOfBoundsException( "Input " + ordinalInParent );
        }
        recomputeDigest();
    }

}
