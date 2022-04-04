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

package org.polypheny.db.adaptiveness;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;

public interface SelfAdaptivAgent {

    @Getter
    Map<String, AlgNode> materializedViews = new HashMap<>();

    default Map<String, AlgNode> getMaterializedViews(){
        return materializedViews;
    }

}