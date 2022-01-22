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

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalAggregate.SerializableAggregate;
import org.polypheny.db.algebra.logical.LogicalFilter.SerializableFilter;
import org.polypheny.db.algebra.logical.LogicalJoin.SerializableJoin;
import org.polypheny.db.algebra.logical.LogicalProject.SerializableProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalSort.SerializableSort;
import org.polypheny.db.algebra.logical.LogicalTableScan.SerializableTableScan;
import org.polypheny.db.algebra.logical.LogicalUnion.SerializableUnion;
import org.polypheny.db.algebra.logical.LogicalValues.SerializableValues;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Permutation;

public abstract class SerializableAlgNode implements Externalizable {

    private static final AtomicInteger NEXT_ID = new AtomicInteger( 0 );
    @Getter
    private final int id;

    @Getter
    private final List<SerializableAlgNode> inputs = new ArrayList<>();


    public SerializableAlgNode() {
        this.id = NEXT_ID.getAndIncrement();
    }


    public static SerializableAlgNode pack( AlgNode node ) {
        SerializableAlgNodeBuilder serializer = new SerializableAlgNodeBuilder();
        node.accept( serializer );
        return serializer.build();
    }


    public void addInput( SerializableAlgNode node ) {
        this.inputs.add( node );
    }


    public abstract void accept( SerializableActivator activator );


    public AlgNode unpack( AlgBuilder builder ) {
        SerializableActivator activator = new SerializableActivator( builder );
        this.accept( activator );
        return activator.build();
    }


    private static class SerializableAlgNodeBuilder extends AlgShuttleImpl {

        private final Queue<SerializableAlgNode> stack = new LinkedList<>();


        public SerializableAlgNode build() {
            if ( stack.size() != 1 ) {
                throw new RuntimeException( "There is more than one input in the serializerBuilder" );
            }
            return stack.poll();
        }


        @Override
        public AlgNode visit( TableScan scan ) {
            stack.add( new SerializableTableScan( scan.getTable().getQualifiedName() ) );
            return scan;
        }


        @Override
        public AlgNode visit( AlgNode other ) {
            if ( other instanceof Filter ) {
                other.getInput( 0 ).accept( this );
                push( new SerializableFilter( ((Filter) other).getCondition() ), 1 );
            } else if ( other instanceof Project ) {
                other.getInput( 0 ).accept( this );
                push( new SerializableProject( ((Project) other).getProjects() ), 1 );
            } else if ( other instanceof Values ) {
                push( new SerializableValues( (AlgRecordType) other.getRowType(), ((Values) other).getTuples() ), 0 );
            } else if ( other instanceof EnumerableLimit ) {
                other.getInput( 0 ).accept( this );
                push( new SerializableSort(
                        getAsLongOrMinus( ((EnumerableLimit) other).offset ),
                        getAsLongOrMinus( ((EnumerableLimit) other).fetch ) ), 1 );
            } else if ( other instanceof Sort ) {
                other.getInput( 0 ).accept( this );
                push( new SerializableSort(
                        ((Sort) other).getCollation(),
                        getAsLongOrMinus( ((Sort) other).offset ),
                        getAsLongOrMinus( ((Sort) other).fetch ) ), 1 );
            } else if ( other instanceof Union ) {
                other.getInputs().forEach( i -> i.accept( this ) );
                push( new SerializableUnion( ((Union) other).all ), other.getInputs().size() );
            } else if ( other instanceof Aggregate ) {
                other.getInput( 0 ).accept( this );
                push( new SerializableAggregate(
                        ((Aggregate) other).indicator,
                        ((Aggregate) other).getGroupSet(),
                        ((Aggregate) other).getGroupSets(),
                        ((Aggregate) other).getAggCallList() ), 1 );
            } else if ( other instanceof Join ) {
                other.getInput( 0 ).accept( this );
                SerializableAlgNode left = stack.poll();
                other.getInput( 1 ).accept( this );
                SerializableAlgNode right = stack.poll();
                push( left, 0 );
                push( right, 0 );
                push( new SerializableJoin( ((Join) other).getJoinType(), ((Join) other).getCondition() ), 2 );
            } else if ( other instanceof Calc ) {
                RexProgram program = ((Calc) other).getProgram();
                other.getInput( 0 ).accept( this );
                RexLocalRef condition = program.getCondition();
                if ( condition != null ) {
                    push( new SerializableFilter( program.expandLocalRef( condition ) ), 1 );
                }

                if ( program.isPermutation() ) {
                    push( new SerializableProject( toProjects( program ) ), 1 );
                }

                List<Pair<RexLocalRef, String>> namedProjects = program.getNamedProjects();
                if ( !program.isPermutation() && condition == null && !namedProjects.isEmpty() ) {
                    push( new SerializableProject( Pair.left( namedProjects ).stream().map( program::expandLocalRef ).collect( Collectors.toList() ), Pair.right( namedProjects ) ), 1 );
                }

            } else {
                if ( other.getInputs().size() > 0 ) {
                    other.getInput( 0 ).accept( this );
                }
            }
            return other;
        }


        private List<RexNode> toProjects( RexProgram program ) {
            Permutation mapping = program.getPermutation().inverse();
            List<RexLocalRef> localProjects = program.getProjectList();
            assert mapping.getMappingType().isSingleSource();
            assert mapping.getMappingType().isMandatorySource();
            if ( mapping.isIdentity() ) {
                return localProjects.stream().map( program::expandLocalRef ).collect( Collectors.toList() );
            }
            final List<RexNode> exprList = new ArrayList<>();
            for ( int i = 0; i < mapping.getTargetCount(); i++ ) {
                exprList.add( program.expandLocalRef( localProjects.get( mapping.getSource( i ) ) ) );
                //exprList.add( field( mapping.getSource( i ) ) );
            }
            return exprList;
        }


        private long getAsLongOrMinus( RexNode node ) {
            RexLiteral value = ((RexLiteral) node);
            return value != null ? (int) (long) value.getValueAs( Long.class ) : -1;
        }


        private void push( SerializableAlgNode node, int amountChildren ) {
            if ( this.stack.size() < amountChildren ) {
                throw new RuntimeException( "Could not correctly serialize algebra as there was a child missmatch." );
            }

            for ( int i = 0; i < amountChildren; i++ ) {
                node.addInput( stack.poll() );
            }

            this.stack.add( node );
        }

    }


    static public class SerializableActivator {

        @Getter
        private final AlgBuilder builder;


        public SerializableActivator( AlgBuilder builder ) {
            this.builder = builder;
        }


        public AlgNode build() {
            if ( builder.stackSize() != 1 ) {
                throw new RuntimeException( "Error while rebuilding AlgNode." );
            }
            return builder.build();
        }


        public void visitChild( SerializableAlgNode node, int index ) {
            node.inputs.get( index ).accept( this );
        }


        public void visit( SerializableFilter filter ) {
            visitChild( filter, 0 );
            builder.filter( filter.getCondition() );
        }


        public void visit( SerializableProject project ) {
            visitChild( project, 0 );
            if ( project.getNames() == null ) {
                builder.project( project.getProjects() );
            } else {
                builder.project( project.getProjects(), project.getNames() );
            }
        }


        public void visit( SerializableSort sort ) {
            visitChild( sort, 0 );
            if ( sort.isOnlyLimit() ) {
                builder.limit( (int) sort.getOffset(), (int) sort.getFetch() );
            } else {
                builder.push(
                        LogicalSort.create(
                                builder.build(),
                                sort.getCollation(),
                                sort.isUsesOffset() ? builder.literal( sort.getOffset() ) : null,
                                sort.isUsesFetch() ? builder.literal( sort.getFetch() ) : null ) );
            }
        }


        public void visit( SerializableTableScan scan ) {
            builder.scan( scan.getNames() );
        }


        public void visit( SerializableUnion union ) {
            union.getInputs().forEach( i -> i.accept( this ) );
            builder.union( union.isAll() );
        }


        public void visit( SerializableValues values ) {
            builder.valuesRows( values.getRowType(), values.getTuples() );
        }


        public void visit( SerializableJoin join ) {
            join.getInputs().get( 0 ).accept( this );
            AlgNode left = builder.build();
            join.getInputs().get( 1 ).accept( this );
            AlgNode right = builder.build();

            builder.push( left );
            builder.push( right );

            builder.join( join.getJoinType(), join.getCondition() );
        }


        public void visit( SerializableAggregate aggregate ) {
            aggregate.getInputs().get( 0 ).accept( this );
            builder.push( new LogicalAggregate(
                    builder.getCluster(),
                    builder.getCluster().traitSet(),
                    builder.build(),
                    aggregate.isIndicator(),
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList() ) );
        }


    }

}
