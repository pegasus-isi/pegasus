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

package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * An abstract base class that provides useful methods for all the 
 * TransformationCatalog Implementations to use.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements TransformationCatalog{
    
    /**
     * Modifies a Transformation Catalog Entry to handle file URL's.
     * A file URL if specified for the physical path is converted to an 
     * absolute path if the type of entry is set to INSTALLED.
     * 
     * @param entry the transformation catalog entry object.
     * 
     * @return the TransformationCatalogEntry object.
     */
    public static TransformationCatalogEntry modifyForFileURLS( TransformationCatalogEntry entry ){
        //sanity checks
        if( entry == null || entry.getPhysicalTransformation() == null ){
            //return without modifying
            return entry;
        }
        
        String url = entry.getPhysicalTransformation();
        //convert file url appropriately for installed executables
        if ( entry.getType().equals( TCType.INSTALLED) &&
                url.startsWith(TransformationCatalog.FILE_URL_SCHEME)) {
            try {
                url = new URL(url).getFile();
            } catch (MalformedURLException ex) {
                throw new RuntimeException("Error while converting file url ", ex);
            }
        }
        entry.setPhysicalTransformation(url);
        return entry;
    }

}
