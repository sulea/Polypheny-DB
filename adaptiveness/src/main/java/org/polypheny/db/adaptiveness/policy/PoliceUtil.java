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

package org.polypheny.db.adaptiveness.policy;

public interface PoliceUtil {

    /**
     * Different Categories are used to describe the different policies used in Polypheny
     */
    enum ClauseCategory {
        STORE, SELF_ADAPTING
    }

    enum AffectedOperations {
        STORE
    }

    enum ClauseName {
        FULLY_PERSISTENT, ONLY_EMBEDDED, ONLY_DOCKER, PERSISTENT, SPEED_OPTIMIZATION, REDUNDANCY_OPTIMIZATION, SPACE_OPTIMIZATION, LANGUAGE_OPTIMIZATION

    }


    enum ClauseType {
        BOOLEAN, NUMBER
    }

    /**
     * Describes for what the policy is used, either only for one table, a store or for everything.
     */
    enum Target {
        ENTITY, NAMESPACE, POLYPHENY
    }



}