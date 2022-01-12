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

package org.polypheny.db.algebra.logical;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Streamer;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

public class LogicalStreamer extends Streamer {

    /**
     * {@code
     * Streamer
     * ^               |
     * |               v
     * Provider    Collector
     * }
     *
     * @param cluster
     * @param traitSet
     * @param provider provides the values which get streamed to the collector
     * @param collector uses the provided values and
     */
    public LogicalStreamer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode provider, AlgNode collector ) {
        super( cluster, traitSet, provider, collector );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalStreamer( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ) );
    }

}