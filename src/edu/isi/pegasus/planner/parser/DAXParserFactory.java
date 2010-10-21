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

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.planner.parser.dax.*;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.common.util.DynamicLoader;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.parser.Parser;
import java.io.File;



/**
 * A factory class to load the appropriate DAX Parser and Callback implementations that need
 * to be passed to the DAX Parser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXParserFactory {
    
    /**
     * The default callback for label partitioning.
     */
    public static String LABEL_CALLBACK_CLASS = "DAX2LabelGraph";

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PARSER_PACKAGE_NAME =
                                             "edu.isi.pegasus.planner.parser.dax";

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_CALLBACK_PACKAGE_NAME =
                                             "edu.isi.pegasus.planner.parser.dax";


    /*
     * Predefined Constant for condor version 7.1.0
     */
    public static final long DAX_VERSION_3_2_0 = CondorVersion.numericValue( "3.2.0" );


    /**
     * The default DAXParser classname
     */
    public static final String DEFAULT_DAX_PARSER_CLASS = "DAXParser3";

    /**
     * The  DAXParser3 classname
     */
    public static final String DAX_PARSER2_CLASS = "DAXParser2";

    /**
     * The  DAXParser3 classname
     */
    public static final String DAX_PARSER3_CLASS = "DAXParser3";

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by
     * the user.
     *
     * @param bag                bag of Pegasus intialization objects
     * @param callbackClass      the dax callback class
     * @param dax file          the dax file
     *
     * @return the DAXParser loaded.
     *
     * @exception DAXParserFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static Parser loadDAXParser( PegasusBag bag, String callbackClass, String daxFile ) throws DAXParserFactoryException{

        PegasusProperties properties = bag.getPegasusProperties();

        //sanity check
        if( properties == null){
            throw new RuntimeException("Invalid properties passed");
        }

        //load the callback
        Callback c = DAXParserFactory.loadDAXParserCallback( properties, daxFile, callbackClass );
        return DAXParserFactory.loadDAXParser( bag, c );

    }

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by
     * the user.
     *
     * @param bag         bag of Pegasus intialization objects
     * @param c           the dax callback.
     *
     * @return the DAXParser loaded.
     *
     * @exception DAXParserFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static Parser loadDAXParser( PegasusBag bag, Callback c  ) throws DAXParserFactoryException{

        String daxClass = DAXParserFactory.DEFAULT_DAX_PARSER_CLASS;
        LogManager logger = bag.getLogger();
        PegasusProperties properties = bag.getPegasusProperties();
        String daxSchema = properties.getDAXSchemaLocation();

        //sanity check
        if( properties == null){
            throw new RuntimeException("Invalid properties passed");
        }
        if( logger == null){
            throw new RuntimeException("Invalid logger passed");
        }

        if( daxSchema != null ){
            //try to determin the version of dax schema
            try{
                String version = null;
                daxSchema = new File( daxSchema ).getName();
                if( daxSchema.startsWith( "dax-" )  && daxSchema.endsWith( ".xsd" ) ){
                    version = daxSchema.substring( daxSchema.indexOf( "dax-" ) + 4,
                                                   daxSchema.lastIndexOf(".xsd") );

                    //append .0 to the version number
                    //to be able to convert to numberic value
                    version = version + ".0";
                    System.out.println( version );
                    if( CondorVersion.numericValue(version) < DAXParserFactory.DAX_VERSION_3_2_0 ){
                        daxClass = DAXParserFactory.DAX_PARSER2_CLASS;
                    }
                    else{
                        daxClass = DAXParserFactory.DAX_PARSER3_CLASS;
                    }
                }
            }
            catch( Exception e ){
                logger.log( "Problem while determining the version of dax" ,
                            LogManager.ERROR_MESSAGE_LEVEL );
            }

            logger.log( "DAX Parser Class to be loaded is " + daxClass,
                        LogManager.DEBUG_MESSAGE_LEVEL );
        }

        Parser daxParser = null;
        try{
            //load the DAX Parser class
            //prepend the package name
            daxClass = ( daxClass.indexOf('.') == -1)?
                        //pick up from the default package
                        DEFAULT_PARSER_PACKAGE_NAME + "." + daxClass:
                        //load directly
                        daxClass;

            DynamicLoader dl  = new DynamicLoader( daxClass );
            Object argList[]  = new Object[1];
            argList[0] = bag;
            daxParser = (Parser)dl.instantiate(argList);

            //set the callback for the DAX Parser
            ((DAXParser)daxParser).setDAXCallback( c );
        }
        catch(Exception e){
            throw new DAXParserFactoryException( "Instantiating DAXParser ",
                                                    daxClass, e);
        }
        return daxParser;
    }

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
     * @exception DAXParserFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     * @see edu.isi.pegasus.planner.common.PegasusProperties#getPartitionerDAXCallback()
     */
    public static Callback loadDAXParserCallback( String type,
                                         PegasusProperties properties,
                                         String dax )
        throws DAXParserFactoryException{

        String callbackClass = null;
        
        //for type label always load DAX2LabelGraph
        if ( type.equalsIgnoreCase("label") ){
            callbackClass = LABEL_CALLBACK_CLASS; //graph with labels populated
        }else{
            //pick up the value passed in properties
            callbackClass = properties.getPartitionerDAXCallback();
        }
        
        return loadDAXParserCallback( properties,  dax, callbackClass );

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
     * @exception DAXParserFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static  Callback loadDAXParserCallback( PegasusProperties properties,
                                          String dax,
                                          String className)
        throws DAXParserFactoryException{

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
                        DEFAULT_CALLBACK_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            DynamicLoader dl  = new DynamicLoader( className);
            Object argList[]  = new Object[2];
            argList[0] = properties;
            argList[1] = dax;
            callback = (Callback)dl.instantiate(argList);
        }
        catch(Exception e){
            throw new DAXParserFactoryException("Instantiating DAXCallback ",
                                                  className, e);
        }
        return callback;
    }


   

}
