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

package org.polypheny.db.algebra.logical;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.SerializableAlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;


/**
 * Sub-class of {@link org.polypheny.db.algebra.core.Sort} not targeted at any particular engine or calling convention.
 */
public final class LogicalSort extends Sort {

    private LogicalSort( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, input, collation, offset, fetch );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalSort by parsing serialized output.
     */
    public LogicalSort( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalSort.
     *
     * @param input Input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    public static LogicalSort create( AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        AlgOptCluster cluster = input.getCluster();
        collation = AlgCollationTraitDef.INSTANCE.canonize( collation );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( collation );
        return new LogicalSort( cluster, traitSet, input, collation, offset, fetch );
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, RexNode offset, RexNode fetch ) {
        return new LogicalSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Getter
    @NoArgsConstructor
    public static class SerializableSort extends SerializableAlgNode {

        private boolean usesOffset;
        private boolean usesFetch;
        private long offset;
        private long fetch;
        private AlgCollation collation;
        private boolean onlyLimit;


        public SerializableSort( long offset, long fetch ) {
            this( null, offset, fetch );
        }


        public SerializableSort( AlgCollation collation, long offset, long fetch ) {
            this.usesOffset = offset >= 0;
            this.usesFetch = fetch >= 0;
            this.offset = offset;
            this.fetch = fetch;
            this.collation = collation;
            this.onlyLimit = collation != null;
        }


        @Override
        public void writeExternal( ObjectOutput out ) throws IOException {
            out.writeLong( offset );
            out.writeLong( fetch );
            out.writeObject( collation );
            out.writeObject( getInputs().get( 0 ) );
        }


        @Override
        public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
            this.offset = in.readLong();
            this.fetch = in.readLong();
            this.collation = (AlgCollation) in.readObject();
            this.usesFetch = fetch != -1;
            this.usesOffset = offset != -1;
            this.onlyLimit = collation != null;
            addInput( (SerializableAlgNode) in.readObject() );
        }


        @Override
        public void accept( SerializableActivator activator ) {
            activator.visit( this );
        }

    }

}

