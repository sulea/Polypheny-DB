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

package org.polypheny.db.processing;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConditionalTableModify;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.transaction.Statement;

public class EnumerableAdjuster {

    // todo dl use shuttle to get all included TableModifies...
    public static AlgRoot adjustModify( AlgRoot root, Statement statement ) {
        if ( root.alg instanceof TableModify ) {
            return AlgRoot.of( LogicalConditionalTableModify.create( (LogicalTableModify) root.alg, statement ), Kind.UPDATE );
        } else {
            ModifyAdjuster adjuster = new ModifyAdjuster( statement );
            root.alg.accept( adjuster );
            return root;
        }
    }


    public static AlgRoot adjustBatch( AlgRoot root, Statement statement ) {
        return AlgRoot.of( LogicalBatchIterator.create( root.alg, statement ), root.kind );
    }


    public static boolean needsAdjustment( AlgNode alg ) {
        if ( alg instanceof TableModify ) {
            return ((TableModify) alg).isUpdate();
        }

        boolean needsAdjustment = false;
        for ( AlgNode input : alg.getInputs() ) {
            needsAdjustment |= needsAdjustment( input );
        }
        return needsAdjustment;
    }


    private static class ModifyAdjuster extends AlgShuttleImpl {

        private final Statement statement;


        private ModifyAdjuster( Statement statement ) {
            this.statement = statement;
        }


        @Override
        public AlgNode visit( LogicalConditionalExecute lce ) {
            if ( lce.getRight() instanceof TableModify ) {
                AlgNode ctm = LogicalConditionalTableModify.create( (LogicalTableModify) lce.getRight(), statement );
                lce.replaceInput( 1, ctm );
                return lce;
            } else {
                return lce.getRight().accept( this );
            }
        }

    }

}
