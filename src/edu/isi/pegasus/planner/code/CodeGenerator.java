/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.code;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.util.Collection;

/**
 * The interface that allows us to plug in various code generators for writing out the concrete
 * plan. Each of Code Generators are dependant upon the underlying workflow executors being used. A
 * Code Generator implementation generates the concrete plan in the input format of the underlying
 * Workflow Executor.
 *
 * <p>The appropriate format can be condor submit files, or some XML description.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public interface CodeGenerator {

    /** The version number associated with this API of Code Generator. */
    public static final String VERSION = "1.5";

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag the bag of initialization objects.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize(PegasusBag bag) throws CodeGeneratorException;

    /**
     * Generates the code for the concrete workflow in the input format of the workflow executor
     * being used.
     *
     * @param dag the concrete workflow.
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException;

    /**
     * Generates the code for a single job in the input format of the workflow executor being used.
     *
     * @param dag the dag of which the job is a part of.
     * @param job the <code>Job</code> object holding the information about that particular job.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode(ADag dag, Job job) throws CodeGeneratorException;

    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon. The monitoring
     * should start only after the output files have been generated. FIXME: It should actually
     * happen after the workflow has been submitted. Eventually should be a separate monitor
     * interface, and submit writers should be loaded by an AbstractFactory.
     *
     * @return boolean indicating whether could successfully start the monitor daemon or not.
     */
    public boolean startMonitoring();

    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset() throws CodeGeneratorException;
}
