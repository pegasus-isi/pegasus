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

package org.griphyn.cPlanner.code.gridstart;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.code.CodeGenerator;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;




/**
 * A factory class to load the appropriate type of Code Generator. The
 * CodeGenerator implementation is used to write out the concrete plan.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class LOFGeneratorFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "org.griphyn.cPlanner.code.gridstart.lof";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String DEFAULT_LOF_GENERATOR =
                    "org.griphyn.cPlanner.code.gridstart.lof.Default";


    /**
     * This method loads the appropriate implementing code generator as specified
     * by the user at runtime. If the megadag mode is specified in the options,
     * then that is used to load the implementing class, overriding the submit
     * mode specified in the properties file.
     *
     *
     * @param bag   the bag of initialization objects.
     * 
     * @param dag   the executable workflow so far.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception LOFGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LOFGenerator loadInstance( PegasusBag bag, ADag dag )
        throws LOFGeneratorFactoryException{

        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();

        //sanity check
        if(properties == null){
            throw new RuntimeException("Invalid properties passed");
        }
        if(options == null){
            throw new RuntimeException("Invalid Options specified");
        }

        
        //pick up the basename/classname from the properties.
        String className = properties.getLOFGeneratorImplementation();
        className = ( className == null ) ? DEFAULT_LOF_GENERATOR : className;


        return loadInstance( bag, dag, className );

    }

    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param bag   the bag of initialization objects.
     * @param dag   the executable workflow so far.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception LOFGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LOFGenerator loadInstance( PegasusBag bag, ADag dag, String className)
        throws LOFGeneratorFactoryException{


        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();


        //sanity check
        if (properties == null) {
            throw new RuntimeException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new RuntimeException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        LOFGenerator lGen = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            lGen = (LOFGenerator) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            lGen.initialize(bag, dag);
        }
        catch (Exception e) {
            throw new LOFGeneratorFactoryException(
                "Instantiating Code Generator ",
                className, e);
        }

        return lGen;
    }

}
