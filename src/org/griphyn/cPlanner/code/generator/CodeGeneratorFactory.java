/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.code.generator;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.code.CodeGenerator;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;


/**
 * A factory class to load the appropriate type of Code Generator. The
 * CodeGenerator implementation is used to write out the concrete plan.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CodeGeneratorFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "org.griphyn.cPlanner.code.generator";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String CONDOR_CODE_GENERATOR_CLASS =
                    "org.griphyn.cPlanner.code.generator.condor.CondorGenerator";


    /**
     * This method loads the appropriate implementing code generator as specified
     * by the user at runtime. If the megadag mode is specified in the options,
     * then that is used to load the implementing class, overriding the submit
     * mode specified in the properties file.
     *
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner at runtime.
     * @param directory  the base directory where the generated code should reside.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see org.griphyn.cPlanner.common.PegasusProperties#getDAXCallback()
     */
    public static CodeGenerator loadInstance(PegasusProperties properties,
                                             PlannerOptions options,
                                             String directory)
        throws CodeGeneratorFactoryException{

        //sanity check
        if(properties == null){
            throw new RuntimeException("Invalid properties passed");
        }
        if(options == null){
            throw new RuntimeException("Invalid Options specified");
        }

        //resolve the basename of the class on the basis of the megadag mode
        String mode = (options == null) ? null: options.getMegaDAGMode();
        String className = null;
        if(mode != null){
            //try to see if a special writer needs to be loaded
            className = (mode.equalsIgnoreCase("daglite"))?
                        "DAGLite":
                         null; //we pick from the properties.
        }

        if(className == null){
            //pick up the basename/classname from the properties.
            String submitMode = properties.getSubmitMode();
            className = ( submitMode.equals( "condor" ) ) ?
                        CONDOR_CODE_GENERATOR_CLASS :
                        submitMode;
        }


        return loadInstance( properties, options, directory, className );

    }

    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner at runtime.
     * @param directory  the base directory where the generated code should reside.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static CodeGenerator loadInstance(PegasusProperties properties,
                                             PlannerOptions options,
                                             String directory,
                                             String className)
        throws CodeGeneratorFactoryException{

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
        CodeGenerator cGen = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            cGen = (CodeGenerator) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            cGen.initialize( properties, directory, options );
        }
        catch (Exception e) {
            throw new CodeGeneratorFactoryException(
                "Instantiating Code Generator ",
                className, e);
        }

        return cGen;
    }

}