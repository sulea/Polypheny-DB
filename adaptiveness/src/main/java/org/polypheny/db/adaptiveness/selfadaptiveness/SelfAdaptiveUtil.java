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

package org.polypheny.db.adaptiveness.selfadaptiveness;

public interface SelfAdaptiveUtil {

    enum Trigger {
        JOIN_FREQUENCY, REPEATING_QUERY, AVG_TIME_CHANGE
    }

    enum AdaptiveKind {

        AUTOMATIC, MANUAL
    }


    /**
     * CREATED: new decision added for the first time
     * ADJUSTED: redo of the decision was done
     * NOT_APPLICABLE: not all involved components are still available
     * OLD_DECISION: since the decision was added to the list it was redone manually
     */
    enum DecisionStatus {
        CREATED, ADJUSTED, NOT_APPLICABLE, OLD_DECISION
    }


}