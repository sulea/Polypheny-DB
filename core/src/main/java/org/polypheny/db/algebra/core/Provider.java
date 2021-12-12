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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class Provider extends AbstractAlgNode {

    @Getter
    private final PolyResult result;
    @Getter
    private final Statement statement;
    @Setter
    @Getter
    private AlgOptTable table;

    @Getter
    private final AlgNode input;
    @Getter
    private final RexNode condition;
    @Getter
    private List<RexNode> updatedValue;


    @Override
    protected AlgDataType deriveRowType() {
        if ( rowType == null ) {
            rowType = input.getRowType();
        }
        return rowType;
    }


    /**
     * Creates an <code>AbstractRelNode</code>.
     *  @param cluster
     * @param traitSet
     * @param statement
     */
    public Provider( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode condition, PolyResult result, Statement statement ) {
        super( cluster, traitSet );
        this.input = input;
        this.condition = condition;
        this.result = result;
        this.statement = statement;
    }


    protected static Enumerable<?> bindEnumerable( PreparedResult prep, Statement statement ) {
        return prep.getBindable( CursorFactory.ARRAY ).bind( statement.getDataContext() );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "[" + input.algCompareString() + "] = " + condition.hashCode();
    }


    public RexNode getEnumerableCondition( RexBuilder rexBuilder ) {
        List<Object> ids = new ArrayList<>();
        List<Object[]> res = new ArrayList<>();
        List<RexNode> updatedValues = new ArrayList<>();
        for ( Object o : result.enumerable( statement.getDataContext() ) ) {
            Object[] row = (Object[]) o;
            res.add( row );
            ids.add( row[0] );
            updatedValues.add( getAsRex( rexBuilder, row[row.length - 1] ) );
        }
        this.updatedValue = updatedValues;
        List<RexNode> conditionIds = new ArrayList<>();
        AlgDataType type = rowType.getFieldList().get( 0 ).getType();
        for ( Object id : ids ) {
            conditionIds.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), rexBuilder.makeInputRef( rowType, 0 ), rexBuilder.makeLiteral( id, type, false ) ) );
        }
        RexNode condition = conditionIds.get( 0 );
        if ( ids.size() > 1 ) {
            condition = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), conditionIds );
        }
        return condition;
    }


    private RexNode getAsRex( RexBuilder rexBuilder, Object obj ) {
        if ( obj instanceof Boolean ) {
            return rexBuilder.makeLiteral( (Boolean) obj );
        } else if ( obj instanceof String ) {
            return rexBuilder.makeLiteral( (String) obj );
        } else {
            return rexBuilder.makeLiteral( obj, rowType.getFieldList().get( 1 ).getType(), false );
        }

    }

}
