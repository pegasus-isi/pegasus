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
package edu.isi.pegasus.planner.provenance.pasoa.pps;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.provenance.pasoa.PPS;
import edu.isi.pegasus.planner.refiner.Refiner;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.pasoa.common.BestPractice;
import org.pasoa.common.Constants;
import org.pasoa.pstructure.GlobalPAssertionKey;
import org.pasoa.pstructure.InteractionKey;
import org.pasoa.pstructure.InteractionPAssertion;
import org.pasoa.pstructure.ObjectID;
import org.pasoa.pstructure.PAssertion;
import org.pasoa.pstructure.Record;
import org.pasoa.pstructure.RelationshipPAssertion;
import org.pasoa.pstructure.SubjectID;
import org.pasoa.storeclient.ClientLib;
import org.pasoa.util.httpsoap.WSAddressEndpoint;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/** Implements the PPS interface for recording documentation from a Pegasus refinement. */
public class Pasoa implements PPS {
    // The current workflow XML serialisation (except for the final footer part: see _xmlFooter
    // below)
    // This is built up cumulatively over time by the refiners providing XML fragments to add
    private String _workflowXML;
    // A count of the number of relationship p-assertions recorded (used to create unique
    // p-assertion IDs)
    private int _relationshipPAssertionCounter;
    // The key for the interaction in which a refiner is invoked
    private InteractionKey _causeKey;
    // The key for the interaction in which a refiner completes
    private InteractionKey _effectKey;
    // The name (URI) of the current refinement step
    private String _refinement;
    // The unique name of the current refinement process, generated from system time
    private String _refinementID;

    // The suffix to the XML workflow serialisation
    // private static final String _xmlFooter = "</workflow>";
    private static final String _xmlFooter = "";

    /**
     * On initialisation, create a ClientLib object for communication with a store, set the store
     * URL and create a namespace-aware DOM document parser.
     */
    public Pasoa() throws Exception {
        _storeProxy = new ClientLib();
        String storeURL = "http://localhost:8080/preserv-1.0";

        _storeRecordURL = new URL(storeURL + "/record");

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        _builder = factory.newDocumentBuilder();
    }

    /**
     * On initialisation, create a ClientLib object for communication with a store, set the store
     * URL and create a namespace-aware DOM document parser.
     */
    public Pasoa(String storeURL) throws Exception {
        _storeProxy = new ClientLib();
        _storeRecordURL = new URL(storeURL + "/record");

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        _builder = factory.newDocumentBuilder();
    }

    //  PPS methods  //

    public String beginWorkflowRefinementStep(
            Refiner workflow, String refinementStepName, boolean firstStep) throws Exception {
        if (firstStep) {
            _workflowXML = workflow.getXMLProducer().toXML();
            _refinementID = Long.toString(System.currentTimeMillis());
            _causeKey = createInteractionKey(_refinementID, refinementStepName, true);
        } else {
            // Record relationships between output of one refinement, input of the one being started
            _causeKey = _effectKey;
            _effectKey = createInteractionKey(_refinementID, refinementStepName, true);
            for (Iterator it = workflow.getWorkflow().jobIterator(); it.hasNext(); ) {
                Job job = (Job) it.next();
                String name = job.getName();
                isIdenticalTo(name, name);
            }
            // Now move on to refinement itself
            _causeKey = _effectKey;
        }
        _effectKey = createInteractionKey(_refinementID, refinementStepName, false);
        _refinement = refinementStepName;
        _relationshipPAssertionCounter = 0;

        // Record the initial invocation of the refiner
        recordInteraction(_workflowXML + _xmlFooter, _causeKey, _refinement, true);

        return _refinementID;
    }

    public void isIdenticalTo(String afterNode, String beforeNode) throws Exception {
        recordRelationship(
                _relationshipPAssertionCounter,
                afterNode,
                _identicalParameter,
                _identicalRelation,
                _causeKey,
                beforeNode,
                _identicalParameter,
                _refinement);
        _relationshipPAssertionCounter += 1;
    }

    public void siteSelectionFor(String afterNode, String beforeNode) throws Exception {
        recordRelationship(
                _relationshipPAssertionCounter,
                afterNode,
                _siteSelectionOutputParameter,
                _siteSelectionRelation,
                _causeKey,
                beforeNode,
                _siteSelectionInputParameter,
                _refinement);
        _relationshipPAssertionCounter += 1;
    }

    public void stagingIntroducedFor(List stagingNodes, String appNode) throws Exception {
        for (Object stagingNode : stagingNodes) {
            recordRelationship(
                    _relationshipPAssertionCounter,
                    stagingNode.toString(),
                    _stagingParameter,
                    _stagingRelation,
                    _causeKey,
                    appNode,
                    _stagedForParameter,
                    _refinement);
            _relationshipPAssertionCounter += 1;
        }
    }

    public void registrationIntroducedFor(String registrationNode, String dataStagingNode)
            throws Exception {
        recordRelationship(
                _relationshipPAssertionCounter,
                dataStagingNode,
                _registrationParameter,
                _registrationRelation,
                _causeKey,
                registrationNode,
                _registrationOfParameter,
                _refinement);
        _relationshipPAssertionCounter += 1;
    }

    public void clusteringOf(String clusteredJob, List jobs) throws Exception {
        for (Object inCluster : jobs) {
            recordRelationship(
                    _relationshipPAssertionCounter,
                    clusteredJob,
                    _clusterParameter,
                    _clusteredRelation,
                    _causeKey,
                    inCluster.toString(),
                    _inClusterParameter,
                    _refinement);
            _relationshipPAssertionCounter += 1;
        }
    }

    public void isPartitionOf(String afterNode, List beforeNode) {
        throw new UnsupportedOperationException();
    }

    public void endWorkflowRefinementStep(Refiner workflow) throws Exception {
        _workflowXML += workflow.getXMLProducer().toXML();
        recordInteraction(_workflowXML + _xmlFooter, _effectKey, _refinement, false);
    }

    //  Utility constants and methods  //

    /**
     * A namespace we can use to identify relationships and concepts defined for Pegasus' provenance
     * data
     */
    // private static final String _namespace = "http://www.isi.edu/pasoa";

    // Relations:
    // Relationships are asserted between workflow nodes before a refinement and
    // those after the refinement.  The former are 'objects' of the relationship,
    // the latter are 'subjects'.  Every relationship has a type which is identified
    // by a URI.
    //
    // For each subject and object of a relationship, the role that each plays
    // in the relationship must be declared, the role type being called the
    // 'parameter name' and identified by a URI.

    /**
     * The identicalTo relationship relates a workflow node before and after a refinement that has
     * not changed during that refinement
     */
    public static final String _identicalRelation = NAMESPACE + "/relations#identicalTo";
    /*
     * In an identical relationship both subject and object play the role of
     * 'item', as in 'this item is identical to that item'.
     */
    public static final String _identicalParameter = NAMESPACE + "/parameters#item";

    /**
     * The site seleciotn relationship relates a job that has had its site selected to that same job
     * before site selection.
     */
    public static final String _siteSelectionRelation = NAMESPACE + "/relations#siteSelectionOf";
    /** The job before site selection plays the 'preselection' role. */
    public static final String _siteSelectionInputParameter =
            NAMESPACE + "/parameters#preselection";
    /** The job after site selection plays the 'postselection' role. */
    public static final String _siteSelectionOutputParameter =
            NAMESPACE + "/parameters#postselection";

    public static final String _stagingRelation = NAMESPACE + "/relations#staging";
    public static final String _stagedForParameter = NAMESPACE + "/parameters#stagedFor";
    public static final String _stagingParameter = NAMESPACE + "/parameters#staging";

    public static final String _registrationRelation = NAMESPACE + "/relations#registration";
    public static final String _registrationOfParameter = NAMESPACE + "/parameters#registrationOf";
    public static final String _registrationParameter = NAMESPACE + "/parameters#registration";

    public static final String _clusteredRelation = NAMESPACE + "/relations#clustered";
    public static final String _inClusterParameter = NAMESPACE + "/parameters#inCluster";
    public static final String _clusterParameter = NAMESPACE + "/parameters#cluster";

    /**
     * A partially refined workflow is specified as an XML document. We represent this as a String
     * object, and for convenience this is the closing tag of that document.
     */
    private static final String _workflowPostfix = "</workflow>";

    /** ClientLib is the primary class by which a client communicates with a provenance store */
    private ClientLib _storeProxy;
    /** The URL of the provenance store Web Service (recording port) */
    private URL _storeRecordURL;
    /** A pre-created DOM XML parser (expensive to create so we do just once) */
    private DocumentBuilder _builder;

    /**
     * Conventionally, we use WS-Addressing to identify the endpoints of an interaction between
     * actors, and this method constructs an XML (DOM) fragment in the WS-Addressing schema for a
     * particular URL.
     *
     * @param address The URL of the endpoint
     * @return An XML (DOM) fragment in WS-Addressing endpoint schema containing the address
     */
    public static Element addressToElement(String address) {
        return new WSAddressEndpoint(address).getElement();
    }

    /**
     * Individual jobs in a workflow are identified by an XML document fragment, called a data
     * accessor, and this method constructs the fragment for a given job ID.
     *
     * @param jobID The job ID
     * @return An XML (DOM) fragment representing a reference to that job in an XML workflow
     *     representation
     */
    public Element createDataAccessor(String jobID) throws IOException, SAXException {
        return toElement("<jobID xmlns = \"" + NAMESPACE + "\">" + jobID + "</jobID>");
    }

    /**
     * Creates an interaction p-assertion asserting that a given partially refined workflow was
     * exchanged between actors.
     *
     * @param workflow The (XML) content of the partially refined workflow
     * @return A JavaBean representation of an interaction p-assertion containing the workflow
     */
    public InteractionPAssertion createInteractionPAssertion(String workflow)
            throws IOException, SAXException {
        return new InteractionPAssertion(
                "1", BestPractice.VERBATIM_STYLE, toElement(workflow + _workflowPostfix));
    }

    /**
     * Creates an interaction key to identify an interaction between two actors.
     *
     * @param refinementID The unique identifier for this workflow refinement (run of Pegasus)
     * @param refinementAddress The URI of the particular refinement step (site selection, cluster
     *     etc.)
     * @param preRefinement True if the interaction is pre-refinement, i.e. from Pegasus to a
     *     refiner, rather than the other way round
     */
    public InteractionKey createInteractionKey(
            String refinementID, String refinementAddress, boolean preRefinement) {
        if (preRefinement) {
            return new InteractionKey(
                    addressToElement(PEGASUS),
                    addressToElement(refinementAddress),
                    refinementID + "Start");
        } else {
            return new InteractionKey(
                    addressToElement(refinementAddress),
                    addressToElement(PEGASUS),
                    refinementID + "End");
        }
    }

    /**
     * Creates a relationship p-assertion between nodes in two partially refined workflows.
     *
     * @param count The index of this relationship p-assertion in the interaction (to support the
     *     requirement that each p-assertion has a unique ID)
     * @param effectJobID The job ID of the subject (effect) of the relationship
     * @param effectParameter The role played by the subject of the relationship
     * @param relationType The type of the relationship
     * @param causeKey The interaction key of the object of the relationship
     * @param causeJobID The job ID of the object (cause) of the relationship
     * @param causeParameter The role played by the object of the relationship
     * @return A RelationshipPAssertion JavaBean representing the relationship p-assertion with the
     *     given arguments
     */
    public RelationshipPAssertion createRelationship(
            int count,
            String effectJobID,
            String effectParameter,
            String relationType,
            InteractionKey causeKey,
            String causeJobID,
            String causeParameter)
            throws IOException, SAXException {
        List<ObjectID> objectIDs = new LinkedList<ObjectID>();
        ObjectID objectID =
                new ObjectID(
                        new GlobalPAssertionKey(causeKey, Constants.RECEIVER_VIEW_TYPE, "1"),
                        effectParameter,
                        createDataAccessor(causeJobID),
                        null);

        objectIDs.add(objectID);

        return new RelationshipPAssertion(
                "RPA" + count,
                new SubjectID("1", createDataAccessor(effectJobID), effectParameter),
                relationType,
                objectIDs);
    }

    public void record(
            PAssertion passertion,
            InteractionKey interactionKey,
            boolean isSender,
            String asserterURL)
            throws Exception {
        if (isSender) {
            _storeProxy.record(
                    new Record(
                            passertion,
                            interactionKey,
                            Constants.SENDER_VIEW_TYPE,
                            addressToElement(asserterURL)),
                    _storeRecordURL);
        } else {
            _storeProxy.record(
                    new Record(
                            passertion,
                            interactionKey,
                            Constants.RECEIVER_VIEW_TYPE,
                            addressToElement(asserterURL)),
                    _storeRecordURL);
        }
    }

    public void recordInteraction(
            InteractionPAssertion passertion,
            InteractionKey interactionKey,
            String refinerType,
            boolean refinementInput)
            throws Exception {
        if (refinementInput) {
            record(passertion, interactionKey, true, PEGASUS);
            record(passertion, interactionKey, false, refinerType);
        } else {
            record(passertion, interactionKey, true, refinerType);
            record(passertion, interactionKey, false, PEGASUS);
        }
    }

    public void recordInteraction(
            String workflow,
            InteractionKey interactionKey,
            String refinerType,
            boolean refinementInput)
            throws Exception {
        recordInteraction(
                createInteractionPAssertion(workflow),
                interactionKey,
                refinerType,
                refinementInput);
    }

    public RelationshipPAssertion recordRelationship(
            int count,
            String effectJobID,
            String effectParameter,
            String relationType,
            InteractionKey causeKey,
            String causeJobID,
            String causeParameter,
            String asserterURL)
            throws Exception {
        RelationshipPAssertion passertion =
                createRelationship(
                        count,
                        effectJobID,
                        effectParameter,
                        relationType,
                        causeKey,
                        causeJobID,
                        causeParameter);
        record(passertion, _effectKey, true, asserterURL);
        return passertion;
    }

    /** Convenience method to parse string represented XML into a DOM XML fragment representation */
    public Element toElement(String xmlAsString) throws IOException, SAXException {
        // System.out.println( "XML as string is " + xmlAsString );
        return _builder.parse(new InputSource(new StringReader(xmlAsString))).getDocumentElement();
    }
}
