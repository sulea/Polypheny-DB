/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog.exceptions;


public class UnknownSchemaException extends CatalogException {

    public UnknownSchemaException( String databaseName, String schemaName ) {
        super( "There is no schema with name '" + schemaName + "' in the database '" + databaseName + "'" );
    }


    public UnknownSchemaException( long databaseId, String schemaName ) {
        super( "There is no schema with name '" + schemaName + "' in the database with the id '" + databaseId + "'" );
    }


    public UnknownSchemaException( long schemaId ) {
        super( "There is no schema with the id '" + schemaId + "'" );
    }

}