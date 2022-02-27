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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;

public interface TypeDefinition<T extends TypeDefinition<T>> {

    TypeSpaceMapping<T, PolyphenyTypeDefinition> getToPolyphenyMapping();

    TypeSpaceMapping<PolyphenyTypeDefinition, T> getFromPolyphenyMapping();

    List<Class<?>> getMappingClasses( PolyType type );

    default List<Class<?>> getMappingClasses( AlgDataTypeField field ) {
        return getMappingClasses( field.getType().getPolyType() );
    }

    default Class<?> getGeneralizedMappingClass( PolyType type ) {
        List<Class<?>> classes = getMappingClasses( type );
        if ( classes.size() > 1 ) {
            return Object.class;
        }
        return classes.get( 0 );
    }

    default boolean needsPolyphenyMapping() {
        return true;
    }

    default boolean classesMatch( AlgDataTypeField f, TypeDefinition<?> source ) {
        List<Class<?>> classes = getMappingClasses( f );
        List<Class<?>> sourceClasses = source.getMappingClasses( f );

        return potentialSubTypesMatch( source, f, this )
                && fullyOverlap( sourceClasses, classes ) && noSpecialCases( classes, sourceClasses );
    }

    static boolean fullyOverlap( List<Class<?>> sourceClasses, List<Class<?>> targetClasses ) {
        return targetClasses.stream().allMatch( c -> sourceClasses.stream().allMatch( c::isAssignableFrom ) );
    }

    static boolean noSpecialCases( List<Class<?>> classes, List<Class<?>> sourceClasses ) {
        // every value can be assigned to String, but this can lead to errors
        // this happens when one value is string
        return (!classes.contains( String.class ) && !sourceClasses.contains( String.class )) ||
                (classes.contains( String.class ) && sourceClasses.contains( String.class ));
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
        return fullyOverlap( source.getMappingClasses( type.getPolyType() ), target.getMappingClasses( type.getPolyType() ) );
    }

    static NlsString getNlsString( String obj ) {
        return new NlsString( obj, StandardCharsets.ISO_8859_1.name(), Collation.IMPLICIT );
    }

}
