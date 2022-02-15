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

package org.polypheny.db.adapter.enumerable;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FieldDeclaration;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.type.mapping.ExternalTypeDefinition;
import org.polypheny.db.type.mapping.PolyphenyTypeDefinition;
import org.polypheny.db.type.mapping.TypeDefinition;
import org.polypheny.db.type.mapping.TypeSpaceMapping;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

public interface EnumerableAdapterAlg extends EnumerableAlg {

    default Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder( false );
        Result res = implementInternal( implementor, pref );

        return getMappingResult( implementor, pref, builder, res, getRowType(), getConvention().getTypeDefinition(), ExternalTypeDefinition.INSTANCE );
    }

    static Result getMappingResult( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder builder, Result res, AlgDataType rowType, TypeDefinition<?> sourceDefinition, TypeDefinition<?> targetDefinition ) {
        // if the classes are assignable we don't need to cast
        boolean needsMapping = rowType
                .getFieldList().stream().anyMatch( f -> !targetDefinition.getMappingClass( f ).isAssignableFrom( sourceDefinition.getMappingClass( f ) ) );

        if ( !needsMapping ) {
            return res;
        }

        TypeSpaceMapping<?, ?> mapping;
        if ( targetDefinition == PolyphenyTypeDefinition.INSTANCE ) {
            // from adapter to Polypheny TypeSpace
            mapping = sourceDefinition.getToPolyphenyMapping();
        } else {
            mapping = targetDefinition.getFromPolyphenyMapping();
        }

        Expression childExp = builder.append( builder.newName( "child_" + System.nanoTime() ), res.block );

        // we have to generalize the result into objects to correctly use the defined mapping methods
        Class<?> javaClass = null;
        if ( rowType.getFieldCount() == 1 ) {
            javaClass = Object.class;
        } else {
            javaClass = Object[].class;
        }

        final PhysType physType = new PhysTypeImpl( implementor.getTypeFactory(), rowType, javaClass, pref.prefer( res.format ), sourceDefinition );
        Type inputJavaType = physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        Expression input = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        InputGetterImpl getter = new InputGetterImpl( Collections.singletonList( Pair.of( input, res.physType ) ) );

        final BlockBuilder builder2 = new BlockBuilder();

        ParameterExpression mapping_ = Expressions.parameter( TypeSpaceMapping.class, builder2.newName( "mapping_" + System.nanoTime() ) );

        // transform the rows with the substituted entries back to the needed format
        List<Expression> expressions = new ArrayList<>();
        for ( AlgDataTypeField field : rowType.getFieldList() ) {

            Expression exp = getter.field( builder2, field.getIndex(), null );

            String method = TypeSpaceMapping.getMethodName( field.getType().getPolyType() );
            exp = Expressions.convert_( exp, Object.class );
            exp = Expressions.call( mapping_, method, exp );

            expressions.add( exp );
        }

        builder2.add( Expressions.return_( null, physType.record( expressions ) ) );
        BlockStatement currentBody = builder2.toBlock();

        FieldDeclaration mappingField = Expressions.fieldDecl( Modifier.PUBLIC | Modifier.FINAL, mapping_, Expressions.constant( mapping ) );

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( childExp, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
                        mappingField,
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_RESET.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_RESET.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CLOSE.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CLOSE.method ) ) ),
                        Expressions.methodDecl(
                                Modifier.PUBLIC,
                                EnumUtils.BRIDGE_METHODS
                                        ? Object.class
                                        : outputJavaType,
                                "current",
                                EnumUtils.NO_PARAMS,
                                currentBody ) ) );

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                EnumUtils.NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of( Expressions.methodDecl( Modifier.PUBLIC, enumeratorType, BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(), EnumUtils.NO_PARAMS, Blocks.toFunctionBlock( body ) ) ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    Result implementInternal( EnumerableAlgImplementor implementor, Prefer pref );

    /**
     * Needed to map the types from the internal type convention to the Polypheny type convention
     */

    AlgDataType getRowType();


}
