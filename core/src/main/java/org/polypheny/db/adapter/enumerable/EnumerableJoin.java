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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgNodes;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.EquiJoin;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.serialize.Serializer;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Join} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableJoin extends EquiJoin implements EnumerableAlg {


    /**
     * Creates an EnumerableJoin.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    protected EnumerableJoin( AlgOptCluster cluster, AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, Set<CorrelationId> variablesSet, JoinAlgType joinType ) throws InvalidAlgException {
        super( cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType );
    }


    /**
     * Creates an EnumerableJoin.
     */
    public static EnumerableJoin create( AlgNode left, AlgNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, Set<CorrelationId> variablesSet, JoinAlgType joinType ) throws InvalidAlgException {
        final AlgOptCluster cluster = left.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE ).replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.enumerableJoin( mq, left, right, joinType ) );
        return new EnumerableJoin( cluster, traitSet, left, right, condition, leftKeys, rightKeys, variablesSet, joinType );
    }


    @Override
    public EnumerableJoin copy( AlgTraitSet traitSet, RexNode condition, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        try {
            return new EnumerableJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, variablesSet, joinType );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        double rowCount = mq.getRowCount( this );

        // Joins can be flipped, and for many algorithms, both versions are viable and have the same cost.
        // To make the results stable between versions of the planner, make one of the versions slightly more expensive.
        switch ( joinType ) {
            case RIGHT:
                rowCount = addEpsilon( rowCount );
                break;
            default:
                if ( AlgNodes.COMPARATOR.compare( left, right ) > 0 ) {
                    rowCount = addEpsilon( rowCount );
                }
        }

        // Cheaper if the smaller number of rows is coming from the LHS. Model this by adding L log L to the cost.
        final double rightRowCount = right.estimateRowCount( mq );
        final double leftRowCount = left.estimateRowCount( mq );
        if ( Double.isInfinite( leftRowCount ) ) {
            rowCount = leftRowCount;
        } else {
            rowCount += Util.nLogN( leftRowCount );
        }
        if ( Double.isInfinite( rightRowCount ) ) {
            rowCount = rightRowCount;
        } else {
            rowCount += rightRowCount;
        }
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    private double addEpsilon( double d ) {
        assert d >= 0d;
        final double d0 = d;
        if ( d < 10 ) {
            // For small d, adding 1 would change the value significantly.
            d *= 1.001d;
            if ( d != d0 ) {
                return d;
            }
        }
        // For medium d, add 1. Keeps integral values integral.
        ++d;
        if ( d != d0 ) {
            return d;
        }
        // For large d, adding 1 might not change the value. Add .1%.
        // If d is NaN, this still will probably not change the value. That's OK.
        d *= 1.001d;
        return d;
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        if ( !(left instanceof Values || right instanceof Values) ) {
            return getOptimizedResult( implementor, pref );
        } else {
            return getDefaultResult( implementor, pref );
        }
    }


    private Result getOptimizedResult( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        boolean preRouteRight = this.getJoinType() != JoinAlgType.LEFT
                || (this.getJoinType() == JoinAlgType.INNER && left.getTable().getRowCount() < right.getTable().getRowCount());

        if ( condition instanceof RexLiteral && condition.isAlwaysTrue() ) {
            // cross product cannot be optimized
            return getDefaultResult( implementor, pref );
        }

        final Result enumerable = implementor.visitChild( this, preRouteRight ? 1 : 0, (EnumerableAlg) (preRouteRight ? right : left), pref );
        Expression exp = builder.append( "enumerable_" + System.nanoTime(), enumerable.block );

        LogicalRebuilder rebuilder = new LogicalRebuilder( getCluster(), getTable() );
        this.accept( rebuilder );
        AlgNode rebuild = rebuilder.builder.build();

        byte[] compressed = Serializer.asCompressedByteArray( rebuild );

        String name = builder.newName( "join_" + System.nanoTime() );
        ParameterExpression nameExpr = Expressions.parameter( byte[].class, name );
        implementor.getNodes().put( nameExpr, compressed );

        Expression result = Expressions.call(
                BuiltInMethod.ROUTE_JOIN_FILTER.method,
                Expressions.constant( DataContext.ROOT ),
                exp,
                nameExpr,
                Expressions.constant( preRouteRight ? PRE_ROUTE.RIGHT : PRE_ROUTE.LEFT ) );
        builder.add( Expressions.return_( null, builder.append( "collector_" + System.nanoTime(), result ) ) );

        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }


    public enum PRE_ROUTE {
        LEFT,
        RIGHT
    }


    public Result getDefaultResult( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableAlg) left, pref );
        Expression leftExpression = builder.append( "left" + System.nanoTime(), leftResult.block );
        final Result rightResult = implementor.visitChild( this, 1, (EnumerableAlg) right, pref );
        Expression rightExpression = builder.append( "right" + System.nanoTime(), rightResult.block );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.preferArray() );
        final PhysType keyPhysType = leftResult.physType.project( leftKeys, JavaRowFormat.LIST );
        return implementor.result(
                physType,
                builder.append(
                                Expressions.call(
                                        leftExpression,
                                        BuiltInMethod.JOIN.method,
                                        Expressions.list(
                                                        rightExpression,
                                                        leftResult.physType.generateAccessor( leftKeys ),
                                                        rightResult.physType.generateAccessor( rightKeys ),
                                                        EnumUtils.joinSelector( joinType, physType, ImmutableList.of( leftResult.physType, rightResult.physType ) ) )
                                                .append( Util.first( keyPhysType.comparer(), Expressions.constant( null ) ) )
                                                .append( Expressions.constant( joinType.generatesNullsOnLeft() ) )
                                                .append( Expressions.constant( joinType.generatesNullsOnRight() ) ) ) )
                        .toBlock() );
    }


    private static class LogicalRebuilder extends AlgShuttleImpl {

        private AlgBuilder builder;
        private final AlgOptCluster cluster;
        private AlgOptSchema algOptSchema;


        public LogicalRebuilder( AlgOptCluster cluster, AlgOptTable algOptTable ) {
            this.algOptSchema = algOptTable == null ? null : algOptTable.getRelOptSchema();
            this.cluster = cluster;
            this.builder = AlgFactories.LOGICAL_BUILDER.create( cluster, algOptSchema );
        }


        @Override
        public AlgNode visit( TableScan scan ) {
            if ( algOptSchema == null ) {
                this.algOptSchema = scan.getTable().getRelOptSchema();
                this.builder = AlgFactories.LOGICAL_BUILDER.create( cluster, algOptSchema );
            }
            builder.scan( getLogicalTableName( scan.getTable() ) );
            return scan;
        }


        private static List<String> getLogicalTableName( AlgOptTable table ) {
            List<String> originalNames = table.getQualifiedName();
            // [name]_[partitionId]
            List<String> names = new ArrayList<>( Arrays.asList( originalNames.get( 1 ).split( "_" ) ) );
            names = names.subList( 0, names.size() - 1 );

            // [storeName]_[name]_[adapter]
            List<String> schemas = new ArrayList<>( Arrays.asList( originalNames.get( 0 ).split( "_" ) ) );
            schemas = schemas.subList( 1, schemas.size() - 1 );
            return Arrays.asList( String.join( "_", schemas ), String.join( "_", names ) );
        }


        @Override
        public AlgNode visit( AlgNode other ) {
            if ( other instanceof Filter ) {
                other.getInput( 0 ).accept( this );
                builder.filter( ((Filter) other).getCondition() );
            } else if ( other instanceof Project ) {
                other.getInput( 0 ).accept( this );
                builder.project( ((Project) other).getProjects() );
            } else if ( other instanceof Values ) {
                builder.valuesRows( other.getRowType(), ((Values) other).getTuples() );
            } else if ( other instanceof EnumerableLimit ) {
                other.getInput( 0 ).accept( this );
                RexLiteral offset = ((RexLiteral) ((EnumerableLimit) other).offset);
                RexLiteral fetch = ((RexLiteral) ((EnumerableLimit) other).fetch);
                builder.limit(
                        offset != null ? (int) (long) offset.getValueAs( Long.class ) : -1,
                        fetch != null ? (int) (long) fetch.getValueAs( Long.class ) : -1 );
            } else if ( other instanceof Sort ) {
                other.getInput( 0 ).accept( this );
                builder.push( LogicalSort.create( builder.build(), ((Sort) other).getCollation(), ((Sort) other).offset, ((Sort) other).fetch ) );
            } else if ( other instanceof Join ) {
                other.getInput( 0 ).accept( this );
                AlgNode left = builder.build();
                other.getInput( 1 ).accept( this );
                AlgNode right = builder.build();
                builder.push( left );
                builder.push( right );

                builder.join( ((Join) other).getJoinType(), ((Join) other).getCondition() );
            } else if ( other instanceof Calc ) {
                RexProgram program = ((Calc) other).getProgram();
                other.getInput( 0 ).accept( this );
                RexLocalRef condition = program.getCondition();
                if ( condition != null ) {
                    builder.filter( program.expandLocalRef( condition ) );
                }

                if ( program.isPermutation() ) {
                    builder.permute( program.getPermutation().inverse() );
                }

                List<Pair<RexLocalRef, String>> namedProjects = program.getNamedProjects();
                if ( !program.isPermutation() && condition == null && !namedProjects.isEmpty() ) {
                    builder.project(
                            Pair.left( namedProjects ).stream().map( program::expandLocalRef ).collect( Collectors.toList() ),
                            Pair.right( namedProjects ) );
                }

            } else {
                if ( other.getInputs().size() > 0 ) {
                    other.getInput( 0 ).accept( this );
                }
            }
            return other;
        }

    }

}

