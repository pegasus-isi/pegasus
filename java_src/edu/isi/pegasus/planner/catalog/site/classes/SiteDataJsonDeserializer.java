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
package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.common.PegasusJsonDeserializer;

/**
 * Abstract Class for Deserializers for parsing Site Catalog YAML Spec
 *
 * @author vahi
 */
/**
 * Custom deserializer for YAML representation of Directory
 *
 * @author Karan Vahi
 */
public abstract class SiteDataJsonDeserializer<T> extends PegasusJsonDeserializer<T> {

    /**
     * The exception to be thrown while deserializing on error
     *
     * @param message the error message
     * @return
     */
    @Override
    public RuntimeException getException(String message) {
        return new SiteCatalogException(message);
    }
}
