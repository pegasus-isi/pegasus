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
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A callback that causes the parser to exit after the metadata about the DAX has been parsed. This
 * is achieved by stopping the parsing after the cbDocument method.
 *
 * @author Karan Vahi
 * @version $Revision: 314 $
 */
public class DAX2Metadata implements Callback {

    /** The parsing completed message. */
    public static final String PARSING_DONE_ERROR_MESSAGE = "Parsing done";

    /** Default attribute value for the count attribute */
    public static final String DEFAULT_ADAG_COUNT_ATTRIBUTE = "1";

    /** Default index value for the count attribute */
    public static final String DEFAULT_ADAG_INDEX_ATTRIBUTE = "0";

    /** The handle to the properties object. */
    private PegasusProperties mProps;

    /** A flag to specify whether the graph has been generated for the partition or not. */
    private boolean mDone;

    /** The metadata of the workflow. */
    private Map mMetadata;

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects containing the properties and the logger
     * @param dax the path to the DAX file.
     */
    public void initialize(PegasusBag bag, String dax) {
        mProps = bag.getPegasusProperties();
        mDone = false;
    }

    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. It ends up storing the attributes with the adag element in the internal memory
     * structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        mMetadata = new HashMap();
        mMetadata.put(
                "count",
                attributes.containsKey("count")
                        ? (String) attributes.get("count")
                        : DEFAULT_ADAG_COUNT_ATTRIBUTE);
        mMetadata.put(
                "index",
                attributes.containsKey("index")
                        ? (String) attributes.get("index")
                        : DEFAULT_ADAG_INDEX_ATTRIBUTE);
        mMetadata.put("name", (String) attributes.get("name"));
        mMetadata.put("version", (String) attributes.get("version"));
        // call the cbDone()
        cbDone();
    }

    /**
     * Callback when a invoke entry is encountered in the top level inside the adag element in the
     * DAX.
     *
     * @param invoke the invoke object
     */
    public void cbWfInvoke(Invoke invoke) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a metadata element is encountered in the adag element.
     *
     * @param profile profile element of namespace metadata
     */
    public void cbMetadata(Profile p) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely assembled, but each is
     * passed separately.
     *
     * @param job the <code>Job</code> object storing the job information gotten from parser.
     */
    public void cbJob(Job job) {}

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, List parents) {}

    /**
     * Callback when the parsing of the document is done. It sets the flag that the parsing has been
     * done, that is used to determine whether the ADag object has been fully generated or not.
     */
    public void cbDone() {
        mDone = true;
        throw new RuntimeException(PARSING_DONE_ERROR_MESSAGE);
    }

    /**
     * Returns an ADag object corresponding to the abstract plan it has generated. It throws a
     * runtime exception if the method is called before the object has been created fully.
     *
     * @return ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject() {
        if (!mDone) throw new RuntimeException("Method called before the metadata was parsed");
        return mMetadata;
    }

    /**
     * Callback when a compound transformation is encountered in the DAX
     *
     * @param compoundTransformation the compound transforamtion
     */
    public void cbCompoundTransformation(CompoundTransformation compoundTransformation) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a replica catalog entry is encountered in the DAX
     *
     * @param rl the ReplicaLocation object
     */
    public void cbFile(ReplicaLocation rl) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a transformation catalog entry is encountered in the DAX
     *
     * @param tce the transformationc catalog entry object.
     */
    public void cbExecutable(TransformationCatalogEntry tce) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cbChildren(String parent, List<String> children) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
                                       // Templates.
    }
}
