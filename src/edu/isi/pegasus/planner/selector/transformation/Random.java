/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.selector.transformation;

import edu.isi.pegasus.planner.selector.TransformationSelector;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.common.PegRandom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * This implemenation of the TCSelector selects a random
 * TransformationCatalogEntry from a List of entries.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Random
    extends TransformationSelector {
    public Random() {
    }

    /**
     * This method randomly selects one of the records from numerous valid
     * Transformation Catalog Entries returned by the TCMapper.
     *
     * @param tcEntries List TransformationCatalogEntry objects returned by the TCMapper.
     * @param preferredSite  the preferred site for selecting the TC entries
     * 
     * @return TransformationCatalogEntry Single TransformationCatalogEntry object
     */
    public List getTCEntry( List<TransformationCatalogEntry> tcEntries, String preferredSite ) {
        
        //prefer entries to select from that are on the preferred site
        List<TransformationCatalogEntry> preferredEntries = new LinkedList();
        for( TransformationCatalogEntry entry: tcEntries ){
            if( entry.getResourceId().equals( preferredSite ) ){
                preferredEntries.add( entry );
            }
        }
        List<TransformationCatalogEntry> selectFrom = preferredEntries.size() > 0 ?
                preferredEntries:
                tcEntries;
        
        TransformationCatalogEntry selected = selectFrom.size() > 1?
                                                //select a random entry
                                                (TransformationCatalogEntry) selectFrom.get( PegRandom.getInteger( selectFrom.size() - 1 )):
                                                //return the first one
                                                (TransformationCatalogEntry) selectFrom.get(0);
        
        
        List result = new ArrayList( 1 );
        result.add( selected );
        return result;
    }

}
