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


package org.griphyn.cPlanner.poolinfo;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.lang.reflect.Method;

/**
 * This class determines at runtime which
 * implementing class to use as a Pool Handle.
 * It uses the reflection package of java
 * to dynamically load a class. The class
 * to be loaded is specified by the
 * vds.pool.mode property.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class PoolMode {

    /**
     * Constants to specify how to load the
     * class, as singleton or non singleton.
     */
    public static final int SINGLETON_LOAD = 0;

    public static final int NON_SINGLETON_LOAD = 1;

    /**
     * Constants for single read.
     *
     */
    public static final String SINGLE_READ = "single";

   // public static final int SINGLE_READ_VALUE = 0;

 //   public static final String SINGLE_READ_CLASS = "SingleReadPool";

    /**
     * Constants for multiple read.
     */
    public static final String MULTIPLE_READ = "multiple";

 //   public static final int MULTIPLE_READ_VALUE = 1;

 //   public static final String MULTIPLE_READ_CLASS = "XML";

    /**
     * Constants for xml pool read.
     */
//    public static final String XML_READ = "xml";

    public static final int XML_READ_VALUE = 2;

    public static final String XML_READ_CLASS = "XML";

    /**
     * Constants for multiline text pool read.
     */
  //  public static final String TEXT_READ = "text";

    public static final int TEXT_READ_VALUE = 3;

    public static final String TEXT_READ_CLASS = "Text";



    /**
     * Constants for mode not defined.
     */
    public static final int UNDEFINED_READ_VALUE = -1;

    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.poolinfo.";

    private static LogManager mLogger = LogManager.getInstance();

    //add your constants here.

    /**
     * Given a string readMode returns the
     * name of the class that implements
     * that read mode. If the readMode
     * doesnt equal any of the predefined
     * constants then the value of readMode
     * is taken as the name of the implementing class.
     *
     * @param readMode  The String form of the
     *              read mode, got from the property
     *              vds.pool.mode.
     *
     * @return  the corresponding int value
     *          of the mode. If not found
     *          then null
     *
     */
    public static String getImplementingClass( String readMode ) {
        if ( readMode.trim().equalsIgnoreCase( SINGLE_READ ) ||
             readMode.trim().equalsIgnoreCase( MULTIPLE_READ )) {
            throw new RuntimeException("The pool mode " + readMode  + " is no " +
                                       "longer supported. Please use the " +
                                       XML_READ_CLASS + " mode or the "+TEXT_READ_CLASS+" mode.");
        } else if ( readMode.trim().equalsIgnoreCase( XML_READ_CLASS ) ) {
            return XML_READ_CLASS;
        } else if ( readMode.trim().equalsIgnoreCase(TEXT_READ_CLASS)){
            return TEXT_READ_CLASS;
        } else {
            //no match to any predefined constant
            //assume that the value of readMode is the
            //name of the implementing class
            return readMode;
        }

    }

    /**
     * Loads the pool info provider class using the reflection package
     * in java at runtime. The class is loaded as a singleton or
     * a non-singleton dependant on the parameter passed. The properties
     * file that is picked up is the default properties file from
     * $PEGASUS_HOME/etc directory.
     *
     * @param poolClass the name of the class that resides in the
     *                  package named PoolMode.PACKAGE_NAME or the
     *                  complete name of the class including the
     *                  package name.
     *
     * @param poolProvider  the path to the file, that contains the
     *                      pool configuration in the appropriate format
     *                      that the implementing poolClass understands.
     *
     *
     * @param lMode     the loading mode of the class. It specifies whether
     *                  the singleton object of the class needs to be
     *                  loaded or the non singleton instance.
     *
     * @return  the object corresponding to the pool info provider class.
     */
    public static PoolInfoProvider loadPoolInstance( String poolClass,
        String poolProvider, int lMode ) {

        Object argList[] = new Object[2 ];
        argList[ 0 ] = poolProvider;
        argList[ 1 ] = org.griphyn.common.util.VDSProperties.PROPERTY_FILENAME;

        return loadPoolInstance( poolClass, lMode, argList );

    }

    /**
     * Loads the pool info provider class using the reflection package
     * in java at runtime. The class is loaded as a singleton or
     * a non-singleton dependant on the parameter passed.
     *
     * @param poolClass the name of the class that resides in the
     *                  package named PoolMode.PACKAGE_NAME or the
     *                  complete name of the class including the
     *                  package name.
     *
     * @param poolProvider  the path to the file, that contains the
     *                      pool configuration in the appropriate format
     *                      that the implementing poolClass understands.
     *
     * @param propFileName  name of the properties file to picked from
     *                      $PEGASUS_HOME/etc/ directory. For the singleton
     *                      loading only the default file is picked up.
     *
     * @param lMode     the loading mode of the class. It specifies whether
     *                  the singleton object of the class needs to be
     *                  loaded or the non singleton instance.
     *
     * @return  the object corresponding to the pool info provider class.
     */
    public static PoolInfoProvider loadPoolInstance( String poolClass,
        String poolProvider, String propFileName, int lMode ) {

        Object argList[] = new Object[2 ];
        argList[ 0 ] = poolProvider;
        argList[ 1 ] = propFileName;

        return loadPoolInstance( poolClass, lMode, argList );

    }



    /**
     * Its returns the name of the method that needs to be invoked
     * to get the object of the implementing pool class. It determines
     * the method name on the basis of the value of the loading mode
     * specified.
     *
     * @param lMode     the loading mode of the class. It specifies whether
     *                  the singleton object of the class needs to be
     *                  loaded or the non singleton instance.
     *
     * @return  the name of the method that needs to be invoked.
     */
    public static String getMethodName( int lMode ) {
        String name = null;
        if ( lMode == SINGLETON_LOAD ) {
            name = "singletonInstance";
        } else {
            if ( lMode == NON_SINGLETON_LOAD ) {
                name = "nonSingletonInstance";
            }
        }
        return name;

    }

    /**
     * Loads the appropriate class that implements a particular
     * pool mode using the reflection package in java at runtime.
     *
     * @param poolClass String
     *
     * @param lMode     the loading mode of the class. It specifies whether
     *                  the singleton object of the class needs to be
     *                  loaded or the non singleton instance.
     * @param argList Object[]
     * @return PoolInfoProvider
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    private static PoolInfoProvider loadPoolInstance( String poolClass,
        int lMode,
        Object[] argList ) throws FactoryException {
        PoolInfoProvider pi = null;
        String mLogMsg = null;
        String methodName = getMethodName( lMode );

        //get the complete name including
        //the package if the package name not
        //specified
        if ( poolClass.indexOf( "." ) == -1 ) {
            poolClass = PACKAGE_NAME + poolClass;
        }

        DynamicLoader d = new DynamicLoader( poolClass );

        try {
            //instantiate the class
            //with no constructor
            Class cls = Class.forName( poolClass );

            //creating a new string to get
            //it's class object that needs to
            //be passed. Could be wrong potentially??
            //This identifies the signature for
            //the method
            Class partypes[] = new Class[argList.length ];

            for ( int i = 0; i < argList.length; i++ ) {
                partypes[ i ] = ( argList[ i ] == null ) ?
                    //try to put in a string
                    //actually the calling class should never pass
                    //null
                    new String().getClass() :
                    argList[ i ].getClass();
            }

            //get the handle to the method
            Method meth = cls.getMethod( methodName, partypes );

            //invoke the method that returns
            //us the singleton instance
            Object retobj = meth.invoke( null, argList );
            pi = ( PoolInfoProvider ) retobj;
        } catch ( Exception e ) {
            throw new FactoryException( "Instantiating Create Directory",
                                        poolClass,
                                        e );
        }

        return pi;
    }

    /**
     * given a string Mode returns the
     * corresponding int value
     *
     * @param readMode  The String form of the
     *              read mode
     *
     * @return  the corresponding int value
     *          of the mode. If not found
     *          then null
     * @deprecated
     */
    public static int getValue( String readMode ) {
        if ( readMode.trim().equalsIgnoreCase( SINGLE_READ ) ) {
            return -1;
        } else if ( readMode.trim().equalsIgnoreCase( MULTIPLE_READ ) ) {
            return -1;
        } else if ( readMode.trim().equalsIgnoreCase( XML_READ_CLASS ) ) {
            return XML_READ_VALUE;
        } else if ( readMode.trim().equalsIgnoreCase( TEXT_READ_CLASS ) ) {
            return TEXT_READ_VALUE;
        } else {
            return UNDEFINED_READ_VALUE;
        }
    }


}
