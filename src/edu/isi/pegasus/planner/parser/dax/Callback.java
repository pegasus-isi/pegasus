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
package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.dax.Invoke;

/**
 * This interfaces defines the callback calls from DAX parsing. A slim and memory-efficient parser
 * of DAX is expected to implement these callbacks, and generate its own information on the fly.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public interface Callback {

    /** The version of the Callback api */
    public static final String VERSION = "1.6";

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects containing the properties and the logger
     * @param dax the path to the DAX file.
     */
    public void initialize(PegasusBag bag, String dax);

    /**
     * Return a object that is constructed during the parsing of the object. The type of the object
     * that is constructed is determined by the implementing callback handler. For example, it could
     * be an Adag object used by Pegasus or a map containing the graph structure of the dax. The
     * implementing classes should keep a boolean flag that signifies whether the corresponding
     * object has been created by the implementing class or not. The variable should be set when the
     * implementing callback handler deems that it has enough data to construct that object.
     *
     * @return constructed object
     */
    public Object getConstructedObject();

    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. This callback can also be used to initialize callback-specific resources.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(java.util.Map attributes);

    /**
     * Callback when a invoke is encountered in the DAX from the top level inside adag tag.
     *
     * @param invoke the invoke object
     */
    public void cbWfInvoke(Invoke invoke);

    /**
     * Callback when a replica catalog entry is encountered in the DAX from Section 1: Files that
     * lists entries in a Replica Catalog
     *
     * @param rl the ReplicaLocation object
     */
    public void cbFile(ReplicaLocation rl);

    /**
     * Callback when a transformation catalog entry is encountered in the DAX from Section 2:
     * Executables that list entries in a Transformaton Catalog
     *
     * @param tce the transformationc catalog entry object.
     */
    public void cbExecutable(TransformationCatalogEntry tce);

    /**
     * Callback when a compound transformation is encountered in the DAX from Section 3: that lists
     * Transformations that Aggregate executables and Files
     *
     * @param compoundTransformation the compound transforamtion
     */
    public void cbCompoundTransformation(CompoundTransformation compoundTransformation);

    /**
     * Callback when a metadata element is encountered in the adag element.
     *
     * @param p profile element of namespace metadata
     */
    public void cbMetadata(Profile p);

    /**
     * Callback for the job from section 4: Job's, DAX's or Dag's that list a JOB or DAX or DAG .
     * These jobs are completely assembled, but each is passed separately.
     *
     * @param job is the DAX-style job.
     */
    public void cbJob(Job job);

    /**
     * Callback for child and parent relationships from Section 5: Dependencies that lists Parent
     * Child relationships (can be empty)
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of edjes denoted by PCRelation object.
     * @deprecated 
     */
    public void cbParents(String child, java.util.List<PCRelation> parents);
    
    /**
     * Callback for child and parent relationships from Section 5: Dependencies that lists Parent
     * Child relationships (can be empty)
     *
     * @param parent is the IDREF of the child element.
     * @param children is a list of id's of children nodes.
     */
    public void cbChildren(String parent, java.util.List<String> children);

    /**
     * Callback when the parsing of the document is done. While this state could also be determined
     * from the return of the invocation of the parser, that return may be hidden in another place
     * of the code. This callback can be used to free callback-specific resources.
     */
    public void cbDone();
}
