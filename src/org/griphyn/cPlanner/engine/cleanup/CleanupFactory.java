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

package org.griphyn.cPlanner.engine.cleanup;

import org.griphyn.cPlanner.classes.PegasusBag;


import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;




/**
 * A factory class to load the appropriate type of Code Generator. The
 * CodeGenerator implementation is used to write out the concrete plan.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CleanupFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "org.griphyn.cPlanner.engine.cleanup";




    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime.
     *
     * @param bag      bag of initialization objects
     *
     *
     * @return instance of a Cleanup CleanupStrategy implementation
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static CleanupStrategy loadCleanupStraegyInstance(  PegasusBag bag ) throws CleanupFactoryException {

        PegasusProperties props = bag.getPegasusProperties();
        if( props == null ){
            throw new CleanupFactoryException( "Properties instance is null " );
        }
        String className = props.getCleanupStrategy();
        
        //prepend the package name
        className = DEFAULT_PACKAGE_NAME + "." + className;

        //try loading the class dynamically
        CleanupStrategy cd = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[ 0 ];
            cd = ( CleanupStrategy ) dl.instantiate( argList );
            cd.initialize( bag,
                           CleanupFactory.loadCleanupImplementationInstance( bag ) );
            
        } catch (Exception e) {
            throw new CleanupFactoryException( "Instantiating Cleanup Strategy",
                                        className,
                                        e );
        }

        return cd;
    }

    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime.
     *
     * @param bag      bag of initialization objects
     *
     *
     * @return instance of a CreateDirecctory implementation
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static CleanupImplementation loadCleanupImplementationInstance(
                                                              PegasusBag bag ) throws CleanupFactoryException {

        PegasusProperties props = bag.getPegasusProperties();
        if( props == null ){
            throw new CleanupFactoryException( "Properties instance is null " );
        }
        String className = props.getCleanupImplementation();
        //for now
        //className = "DefaultImplementation";
        
        //prepend the package name
        className = DEFAULT_PACKAGE_NAME + "." + className;

        //try loading the class dynamically
        CleanupImplementation impl = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[ 0 ];
            impl = ( CleanupImplementation ) dl.instantiate( argList );
            impl.initialize( bag );
            
        } catch (Exception e) {
            throw new CleanupFactoryException( "Instantiating Cleanup Implementation",
                                        className,
                                        e );
        }

        return impl;
    }
                    

}
