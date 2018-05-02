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

package edu.isi.pegasus.planner.selector;

/**
 *
 * This class is an abstract class for the Transformation Catalog Selector.
 * Its purpose is to provide a generic api to select one valid transformation
 * among the many transformations.
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FactoryException;

import java.util.List;

public abstract class TransformationSelector {

    public static final String PACKAGE_NAME =
        "edu.isi.pegasus.planner.selector.transformation";

    protected LogManager mLogger;

    public TransformationSelector() {
        mLogger =  LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Takes a list of TransformationCatalogEntry objects and returns 1 or many
     * TransformationCatalogEntry objects as a list depending on the type of selection algorithm.
     * The Random and RoundRobin implementation ensure that only one entry is
     * returned and should be run last when chaining multiple selectors
     * 
     * @param tcentries      List
     * @param preferredSite  the preferred site for selecting the TC entries
     * 
     * @return List
     */
    public abstract List getTCEntry( List tcentries, String preferredSite );

    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime in the properties file.
     *
     * @param className  String The name of the class that implements the mode.
     *                   It is the name of the class, not the complete name with
     *                   package. That  is added by itself.
     *
     * @return TransformationSelector
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static TransformationSelector loadTXSelector( String className )
           throws FactoryException {

        //prepend the package name
        className = PACKAGE_NAME + "." + className;

        //try loading the class dynamically
        TransformationSelector ss = null;
        DynamicLoader dl = new DynamicLoader( className );
        try {
            Object argList[] = new Object[0 ];
            //argList[ 0 ] = ( path == null ) ? new String() : path;
            ss = ( TransformationSelector ) dl.instantiate( argList );
        } catch ( Exception e ) {
            throw new FactoryException( "Instantiating Create Directory",
                                        className,
                                        e );
        }

        return ss;
    }

}
