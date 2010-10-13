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

package org.griphyn.cPlanner.parser.dax;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.common.util.DynamicLoader;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;


/**
 * A factory class to load the appropriate DAX callback implementations that need
 * to be passed to the DAX Parser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXCallbackFactory {
    
    /**
     * The default callback for label partitioning.
     */
    public static String LABEL_CALLBACK_CLASS = "DAX2LabelGraph";

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                             "org.griphyn.cPlanner.parser.dax";

    /**
     * Loads the implementing class corresponding to the type specified by the user.
     * The properties object passed should not be null. The callback that is
     * loaded, is the one referred to in the properties by the user, unless the
     * type of partitioning is label. In that case DAX2LabelGraph is loaded always.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param type       the type of partitioning the user specified.
     * @param dax        the path to the DAX file that has to be parsed.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception DAXCallbackFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see org.griphyn.cPlanner.common.PegasusProperties#getPartitionerDAXCallback()
     */
    public static Callback loadInstance( String type,
                                         PegasusProperties properties,
                                         String dax )
        throws DAXCallbackFactoryException{

        String callbackClass = null;
        
        //for type label always load DAX2LabelGraph
        if ( type.equalsIgnoreCase("label") ){
            callbackClass = LABEL_CALLBACK_CLASS; //graph with labels populated
        }else{
            //pick up the value passed in properties
            callbackClass = properties.getPartitionerDAXCallback();
        }
        
        return loadInstance( properties,  dax, callbackClass );

    }


    /**
     * Loads the implementing class corresponding to the type specified by the user.
     * The properties object passed should not be null. The callback that is
     * loaded, is the one referred to by the className parameter passed.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param dax        the path to the DAX file that has to be parsed.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception DAXCallbackFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static  Callback loadInstance( PegasusProperties properties,
                                          String dax,
                                          String className)
        throws DAXCallbackFactoryException{

        //try loading the class dynamically
        Callback callback = null;

        try{
            //sanity check
            if(properties == null){
                throw new RuntimeException("Invalid properties passed");
            }
            if(className == null){
                throw new RuntimeException("Invalid class specified to load");
            }

            //prepend the package name
            className = (className.indexOf('.') == -1)?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            DynamicLoader dl  = new DynamicLoader( className);
            Object argList[]  = new Object[2];
            argList[0] = properties;
            argList[1] = dax;
            callback = (Callback)dl.instantiate(argList);
        }
        catch(Exception e){
            throw new DAXCallbackFactoryException("Instantiating DAXCallback ",
                                                  className, e);
        }
        return callback;
    }


}
