/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.catalog.classes;

import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.common.PegasusJsonDeserializer;

/**
 * Abstract Class for Deserializers for parsing Catalog Data Objects
 *
 * @author Karan Vahi
 * @param <T>
 */
public abstract class CatalogEntryJsonDeserializer<T> extends PegasusJsonDeserializer<T> {

    /**
     * The exception to be thrown while deserializing on error
     *
     * @param message the error message
     *
     * @return
     */
    @Override
    public RuntimeException getException(String message){
        return new CatalogException(message);
    }

    
    
}
