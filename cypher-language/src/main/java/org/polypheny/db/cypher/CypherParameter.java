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

package org.polypheny.db.cypher;

import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherParameter extends CypherExpression {

    private CypherVariable variable;
    private final ParameterType type;
    private String name;


    protected CypherParameter( ParserPos pos, CypherVariable variable, ParameterType type ) {
        super( pos );
        this.variable = variable;
        this.type = type;
    }

    protected CypherParameter( ParserPos pos, String name, ParameterType type ) {
        super( pos );
        this.variable = variable;
        this.name = name;
        this.type = type;
    }

}