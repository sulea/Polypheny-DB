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

package org.polypheny.db.dql;

import org.polypheny.db.CypherList;
import org.polypheny.db.CypherNode;
import org.polypheny.db.languages.ParserPos;

public class CypherReturn extends CypherNode {

    private final CypherNode limit;
    private final CypherNode order;
    private final CypherList<CypherReturnExpr> returns;


    protected CypherReturn( ParserPos pos, CypherList<CypherReturnExpr> returns, CypherNode limit, CypherNode order ) {
        super( pos, EMPTY_CYPHER );
        this.returns = returns;
        this.limit = limit;
        this.order = order;
    }

}