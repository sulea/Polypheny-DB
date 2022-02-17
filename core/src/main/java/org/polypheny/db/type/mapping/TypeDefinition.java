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

package org.polypheny.db.type.mapping;

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.type.PolyType;

public interface TypeDefinition<T extends TypeDefinition<T>> {

    TypeSpaceMapping<T, PolyphenyTypeDefinition> getToPolyphenyMapping();

    TypeSpaceMapping<PolyphenyTypeDefinition, T> getFromPolyphenyMapping();

    Class<?> getMappingClass( PolyType type, boolean nullable );

    default Class<?> getMappingClass( AlgDataTypeField field ) {
        return getMappingClass( field.getType().getPolyType(), field.getType().isNullable() );
    }


    default boolean needsPolyphenyMapping() {
        return true;
    }

    default boolean classesMatch( AlgDataTypeField f, TypeDefinition<?> source ) {
        Class<?> clazz = getMappingClass( f );
        Class<?> sourceClazz = source.getMappingClass( f );

        return potentialSubTypesMatch( source, f, this ) && clazz.isAssignableFrom( sourceClazz ) && noSpecialCases( clazz, sourceClazz );
    }

    static boolean noSpecialCases( Class<?> clazz, Class<?> sourceClazz ) {
        // every value can be assigned to String, but this can lead to errors
        // this happens when one value is string
        return (clazz != String.class && sourceClazz != String.class) ||
                (clazz == String.class && sourceClazz == String.class);
    }

    static boolean potentialSubTypesMatch( TypeDefinition<?> source, AlgDataTypeField f, TypeDefinition<?> target ) {
        boolean temp = true;
        if ( f.getType().getPolyType() == PolyType.ARRAY ) {
            temp = subClassesMatch( source, f.getType().getComponentType(), target );
        } else if ( f.getType().getPolyType() == PolyType.MAP ) {
            temp = subClassesMatch( source, f.getType().getKeyType(), target );
            temp &= subClassesMatch( source, f.getType().getValueType(), target );
        }
        return temp;
    }

    static boolean subClassesMatch( TypeDefinition<?> source, AlgDataType type, TypeDefinition<?> target ) {
        return source
                .getMappingClass( type.getPolyType(), type.isNullable() )
                .isAssignableFrom( target.getMappingClass( type.getPolyType(), type.isNullable() ) );
    }

}
