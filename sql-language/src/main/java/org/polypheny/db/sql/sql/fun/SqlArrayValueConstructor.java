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

package org.polypheny.db.sql.sql.fun;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.ArrayValueConstructor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.sql.SqlBasicCall;
import org.polypheny.db.sql.sql.SqlCall;
import org.polypheny.db.sql.sql.SqlLiteral;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.PolyCollections.PolyList;
import org.polypheny.db.util.PolySerializer;


/**
 * Definition of the SQL:2003 standard ARRAY constructor, <code>ARRAY[&lt;expr&gt;, ...]</code>.
 */
public class SqlArrayValueConstructor extends SqlMultisetValueConstructor implements ArrayValueConstructor {

    public final int dimension;
    public final int maxCardinality;
    public boolean outermost = true;


    public SqlArrayValueConstructor() {
        this( 0, 0, 0 );
    }


    /**
     * Constructor
     *
     * @param dimension The dimension of this array. The most nested array has dimension 1, its parent has dimension 2 etc.
     * @param cardinality The cardinality of the array
     * @param maxCardinality If an array consists of nested arrays that have a larger cardinality, this value will be larger than the array's <i>cardinality</i>.
     */
    public SqlArrayValueConstructor( final int dimension, final int cardinality, final int maxCardinality ) {
        super( "ARRAY", Kind.ARRAY_VALUE_CONSTRUCTOR );
        this.dimension = dimension;
        this.maxCardinality = Math.max( maxCardinality, cardinality );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        AlgDataType type = getComponentType( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createArrayType( opBinding.getTypeFactory(), type, false, dimension, maxCardinality );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        AlgDataType type = super.deriveType( validator, scope, call );
        if ( type instanceof ArrayType ) {
            ((ArrayType) type).setCardinality( maxCardinality ).setDimension( dimension );
        }
        //set the operator again, because SqlOperator.deriveType will clear the dimension & cardinality of this constructor
        ((SqlBasicCall) call).setOperator( this );
        return type;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( !writer.getDialect().supportsNestedArrays() ) {
            PolyList<Comparable<?>> list = createListForArrays( call.getSqlOperandList() );
            writer.literal( "'" + PolySerializer.serializeArray( list ) + "'" );
        } else {
            super.unparse( writer, call, leftPrec, rightPrec );
        }
    }


    private PolyList<Comparable<?>> createListForArrays( List<SqlNode> operands ) {
        PolyList<Comparable<?>> list = new PolyList<>( operands.size() );
        for ( SqlNode node : operands ) {
            if ( node instanceof SqlLiteral ) {
                Comparable<?> value;
                switch ( ((SqlLiteral) node).getTypeName() ) {
                    case CHAR:
                    case VARCHAR:
                        value = ((SqlLiteral) node).toValue();
                        break;
                    case BOOLEAN:
                        value = ((SqlLiteral) node).booleanValue();
                        break;
                    case DECIMAL:
                        value = ((SqlLiteral) node).bigDecimalValue();
                        break;
                    default:
                        value = (Comparable<?>) ((SqlLiteral) node).getValue();
                }
                list.add( value );
            } else if ( node instanceof SqlCall ) {
                list.add( createListForArrays( ((SqlCall) node).getSqlOperandList() ) );
            } else {
                throw new RuntimeException( "Invalid array" );
            }
        }
        return list;
    }


    @Override
    public int hashCode() {
        return Objects.hash( kind, "ARRAY" );
    }


}
