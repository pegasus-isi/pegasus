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
import edu.isi.pegasus.planner.dax.Invoke;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An example callback that prints out the various elements in the DAX.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ExampleDAXCallback implements Callback {
    private boolean mDone;

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects containing the properties and the logger
     * @param dax the path to the DAX file.
     */
    public void initialize(PegasusBag bag, String dax) {}

    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. It ends up storing the attributes with the adag element in the internal memory
     * structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        System.out.println("The attributes in DAX header retrieved ");
        System.out.println(attributes);
    }

    /**
     * Callback when a invoke entry is encountered in the top level inside the adag element in the
     * DAX.
     *
     * @param invoke the invoke object
     */
    public void cbWfInvoke(Invoke invoke) {

        System.out.println();
        System.out.println("Invoke Entry ");
        System.out.println(invoke);
    }

    /**
     * Callback when a metadata element is encountered in the adag element.
     *
     * @param profile profile element of namespace metadata
     */
    public void cbMetadata(Profile p) {
        System.out.println();
        System.out.println("metadata Entry ");
        System.out.println(p);
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely assembled, but each is
     * passed separately.
     *
     * @param job the <code>Job</code> object storing the job information gotten from parser.
     */
    public void cbJob(Job job) {
        System.out.println();
        System.out.println("Job parsed ");
        System.out.println(job);
    }

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, List parents) {
        System.out.println();
        System.out.println("Edges in the DAX ");
        for (Iterator it = parents.iterator(); it.hasNext(); ) {
            System.out.println(it.next() + " -> " + child);
        }
    }

    /**
     * Callback when the parsing of the document is done. It sets the flag that the parsing has been
     * done, that is used to determine whether the ADag object has been fully generated or not.
     */
    public void cbDone() {
        mDone = true;
    }

    /**
     * Returns an ADag object corresponding to the abstract plan it has generated. It throws a
     * runtime exception if the method is called before the object has been created fully.
     *
     * @return ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject() {
        // RETURN YOUR CONVERTED OBJECT HERE
        return new String("Shallow Object");
    }

    /**
     * Callback when a compound transformation is encountered in the DAX
     *
     * @param compoundTransformation the compound transforamtion
     */
    public void cbCompoundTransformation(CompoundTransformation compoundTransformation) {
        System.out.println();
        System.out.println("CompoundTransformation ");
        System.out.println(compoundTransformation);
    }

    /**
     * Callback when a replica catalog entry is encountered in the DAX
     *
     * @param rl the ReplicaLocation object
     */
    public void cbFile(ReplicaLocation rl) {
        System.out.println();
        System.out.println("Replica Catalog Entry ");
        System.out.println(rl);
    }

    /**
     * Callback when a transformation catalog entry is encountered in the DAX
     *
     * @param tce the transformationc catalog entry object.
     */
    public void cbExecutable(TransformationCatalogEntry tce) {

        System.out.println();
        System.out.println("TransformationCatalogEntry Entry ");
        System.out.println(tce);
    }

    @Override
    public void cbChildren(String parent, List<String> children) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
