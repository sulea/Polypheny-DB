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

package org.polypheny.db.cypher.expression;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherLiteral extends CypherExpression {

    private final Literal literalType;
    private final Object value;


    public CypherLiteral( ParserPos pos, Literal literalType ) {
        super( pos );
        this.literalType = literalType;
        this.value = null;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<CypherExpression> list ) {
        super( pos );
        this.literalType = literalType;
        assert literalType == Literal.LIST;
        this.value = list;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String string ) {
        super( pos );
        this.literalType = literalType;
        assert literalType == Literal.STRING;
        this.value = string;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<StringPos> keys, List<CypherExpression> values ) {
        super( pos );
        this.literalType = literalType;
        assert keys.size() == values.size();
        //noinspection UnstableApiUsage
        this.value = Streams.zip( keys.stream(), values.stream(), Maps::immutableEntry ).collect( Collectors.toMap( k -> k.getKey().getImage(), Entry::getValue ) );
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String image, boolean negated ) {
        super( pos );
        this.literalType = literalType;
        if ( literalType == Literal.DECIMAL ) {
            this.value = Float.parseFloat( image ) * (negated ? -1 : 1);
        } else if ( (literalType == Literal.DOUBLE) ) {
            this.value = Double.parseDouble( image ) * (negated ? -1 : 1);
        } else {
            throw new RuntimeException( "Could not use provided format to creat cypher literal." );
        }
    }


    public enum Literal {
        TRUE, FALSE, NULL, LIST, MAP, STRING, DOUBLE, DECIMAL, HEX, OCTAL, STAR
    }

}