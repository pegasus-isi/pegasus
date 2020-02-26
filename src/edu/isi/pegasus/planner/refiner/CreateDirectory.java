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
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.refiner.createdir.Implementation;
import edu.isi.pegasus.planner.refiner.createdir.Strategy;

/**
 * This common interface that identifies the basic functions that need to be implemented to
 * introduce random directories in which the jobs are executed on the remote execution pools. The
 * implementing classes are invoked when the user gives the --randomdir option. The implementing
 * classes determine where in the graph the nodes creating the random directories are placed and
 * their dependencies with the rest of the nodes in the graph.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class CreateDirectory extends Engine {

    /** The name of the package in which all the implementing classes are. */
    public static final String PACKAGE_NAME = "edu.isi.pegasus.planner.refiner.createdir";

    /** It is a reference to the Concrete Dag so far. */
    protected ADag mCurrentDag;

    /**
     * Loads the implementing class corresponding to the mode specified by the user at runtime.
     *
     * @param bag bag of initialization objects
     * @return instance of a CreateDirecctory implementation
     * @throws FactoryException that nests any error that might occur during the instantiation of
     *     the implementation.
     */
    public static Strategy loadCreateDirectoryStraegyInstance(PegasusBag bag)
            throws FactoryException {

        PegasusProperties props = bag.getPegasusProperties();
        if (props == null) {
            throw new FactoryException("Properties instance is null ");
        }
        String className = props.getCreateDirClass();

        // prepend the package name
        className = PACKAGE_NAME + "." + className;

        // try loading the class dynamically
        Strategy cd = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[0];
            cd = (Strategy) dl.instantiate(argList);
            cd.initialize(bag, CreateDirectory.loadCreateDirectoryImplementationInstance(bag));

        } catch (Exception e) {
            throw new FactoryException("Instantiating Create Directory Strategy", className, e);
        }

        return cd;
    }

    /**
     * Loads the implementing class corresponding to the mode specified by the user at runtime.
     *
     * @param bag bag of initialization objects
     * @return instance of a CreateDirecctory implementation
     * @throws FactoryException that nests any error that might occur during the instantiation of
     *     the implementation.
     */
    public static Implementation loadCreateDirectoryImplementationInstance(PegasusBag bag)
            throws FactoryException {

        PegasusProperties props = bag.getPegasusProperties();
        if (props == null) {
            throw new FactoryException("Properties instance is null ");
        }
        String className = props.getCreateDirImplementation();
        // for now
        // className = "DefaultImplementation";

        // prepend the package name
        className = PACKAGE_NAME + "." + className;

        // try loading the class dynamically
        Implementation impl = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[0];
            impl = (Implementation) dl.instantiate(argList);
            impl.initialize(bag);

        } catch (Exception e) {
            throw new FactoryException("Instantiating Create Directory", className, e);
        }

        return impl;
    }

    /**
     * A pratically nothing constructor !
     *
     * @param bag bag of initialization objects
     */
    protected CreateDirectory(PegasusBag bag) {
        super(bag);
    }

    /**
     * It modifies the concrete dag passed in the constructor and adds the create random directory
     * nodes to it at the root level. These directory nodes have a common child that acts as a
     * concatenating job and ensures that Condor does not start staging in the data before the
     * directories have been added. The root nodes in the unmodified dag are now chidren of this
     * concatenating dummy job.
     *
     * @param dag the workflow to which nodes have to be added
     * @return workflow with nodes added.
     */
    public ADag addCreateDirectoryNodes(ADag dag) {
        Strategy s = CreateDirectory.loadCreateDirectoryStraegyInstance(mBag);
        return s.addCreateDirectoryNodes(dag);
    }
}
