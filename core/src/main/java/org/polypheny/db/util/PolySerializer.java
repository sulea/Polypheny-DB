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

package org.polypheny.db.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.NlsString.NlsNormalizeSerializer;
import org.polypheny.db.util.PolyCollections.PolyMap;

public class PolySerializer {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter( NlsString.class, new NlsNormalizeSerializer() )
            .serializeNulls()
            .enableComplexMapKeySerialization()
            .create();


    public static Object reparse( PolyType innerType, Long dimension, String stringValue ) {
        Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        if ( stringValue == null ) {
            return null;
        }
        return gson.fromJson( stringValue.trim(), conversionType );
    }


    public static String parseArray( Object obj ) {
        return gson.toJson( obj );
    }


    public static String parseMap( PolyMap<?, ?> map ) {
        return gson.toJson( map );
    }


    public static PolyMap<?, ?> unparseMap( String obj ) {
        return gson.fromJson( obj, PolyMap.class );
    }

}
