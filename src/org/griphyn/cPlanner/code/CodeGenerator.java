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

package org.griphyn.cPlanner.code;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;


/**
 * The interface that allows us to plug in various code generators for writing
 * out the concrete plan. Each of Code Generators are dependant upon the
 * underlying workflow executors being used. A Code Generator implementation
 * generates the concrete plan in the input format of the underlying Workflow
 * Executor.
 *
 * The appropriate format can be condor submit files, or some XML description.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public interface CodeGenerator {

    /**
     * The version number associated with this API of Code Generator.
     */
    public static final String VERSION = "1.3";


    /**
     * Initializes the Code Generator implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param directory  the base directory where the generated code should reside.
     * @param options    the options passed to the planner at runtime.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            String directory,
                            PlannerOptions options) throws CodeGeneratorException;

    /**
     * Generates the code for the concrete workflow in the input format of the
     * workflow executor being used.
     *
     * @param dag  the concrete workflow.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag ) throws CodeGeneratorException;

    /**
     * Generates the code for a single job in the input format of the workflow
     * executor being used.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>SubInfo</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, SubInfo job ) throws CodeGeneratorException;

    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon.
     * The monitoring should start only after the output files have been generated.
     * FIXME: It should actually happen after the workflow has been submitted.
     *        Eventually should be a separate monitor interface, and submit writers
     *        should be loaded by an AbstractFactory.
     *
     * @return boolean indicating whether could successfully start the monitor
     *         daemon or not.
     */
    public boolean startMonitoring();

    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset( )throws CodeGeneratorException;


}