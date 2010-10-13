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

package edu.isi.pegasus.planner.transfer.sls;


import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.util.DynamicLoader;



/**
 * A factory class to load the appropriate type of SLS Implementation to do
 * the Second Level Staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SLSFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "edu.isi.pegasus.planner.transfer.sls";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String DEFAULT_SLS_IMPL_CLASS  =  "Transfer3";


    /**
     * This method loads the appropriate implementing code generator as specified
     * by the user at runtime. If the megadag mode is specified in the options,
     * then that is used to load the implementing class, overriding the submit
     * mode specified in the properties file.
     *
     *
     * @param bag   the bag of initialization objects.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     *
     * @throws SLSFactoryException
     */
    public static SLS loadInstance( PegasusBag bag )  throws SLSFactoryException{

        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();

        //sanity check
        if(properties == null){
            throw new SLSFactoryException( "Invalid properties passed" );
        }
        if(options == null){
            throw new SLSFactoryException( "Invalid Options specified" );
        }

        String className = properties.getSLSTransferImplementation();
        if( className == null ){
            className = DEFAULT_SLS_IMPL_CLASS; //to be picked up from properties eventually
        }

        return loadInstance( bag, className );

    }

    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param bag   the bag of initialization objects.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     *
     * @throws SLSFactoryException
     */
    public static SLS loadInstance( PegasusBag bag, String className)
        throws SLSFactoryException{


        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();


        //sanity check
        if (properties == null) {
            throw new SLSFactoryException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new SLSFactoryException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        SLS sls = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            sls = ( SLS ) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            sls.initialize( bag );
        }
        catch ( Exception e ) {
            throw new SLSFactoryException( "Instantiating SLS Implementor ", className, e );
        }

        return sls;
    }

}
