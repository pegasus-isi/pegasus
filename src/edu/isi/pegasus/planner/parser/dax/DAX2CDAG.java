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

package edu.isi.pegasus.planner.parser.dax;


import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.DagInfo;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.classes.ReplicaStore;
import edu.isi.pegasus.planner.common.PegasusProperties;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * This creates a dag corresponding to one particular partition of the whole
 * abstract plan. The partition can be as big as the whole abstract graph or can
 * be as small as a single job. The partitions are determined by the Partitioner.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAX2CDAG implements Callback {

    /**
     * The DAGInfo object which contains information corresponding to the ADag in
     * the XML file.
     */
    private DagInfo mDagInfo;

    /**
     * Contains Job objects. One per submit file.
     */
    private Vector mVSubInfo;

    /**
     * The mapping of the idrefs of a job to the job name.
     */
    private Map mJobMap;

    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * A flag to specify whether the graph has been generated for the partition
     * or not.
     */
    private boolean mDone;

    /**
     * Handle to the replica store that stores the replica catalog
     * user specifies in the DAX
     */
    protected ReplicaStore mReplicaStore;


    /**
     * Handle to the transformation store that stores the transformation catalog
     * user specifies in the DAX
     */
    protected TransformationStore mTransformationStore;

    /**
     * Map of Compound Transfomations indexed by complete name of the compound
     * transformation.
     */
    protected Map<String,CompoundTransformation> mCompoundTransformations;


    /**
     * The overloaded constructor.
     *
     * @param properties  the properties passed to the planner.
     * @param dax         the path to the DAX file.
     */
    public DAX2CDAG( PegasusProperties properties, String dax ) {
//        mDAXPath      = dax;
        mDagInfo      = new DagInfo();
        mVSubInfo     = new Vector();
        mJobMap       = new HashMap();
        mProps        = properties;
        mDone         = false;
        this.mReplicaStore = new ReplicaStore();
        this.mTransformationStore = new TransformationStore();
        this.mCompoundTransformations = new HashMap<String,CompoundTransformation>();
    }


    /**
     * Callback when the opening tag was parsed. This contains all
     * attributes and their raw values within a map. It ends up storing
     * the attributes with the adag element in the internal memory structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        mDagInfo.count = (String)attributes.get("count");
        mDagInfo.index = (String)attributes.get("index");
        mDagInfo.setLabel( (String)attributes.get("name") );
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely
     * assembled, but each is passed separately.
     *
     * @param job  the <code>Job</code> object storing the job information
     *             gotten from parser.
     */
    public void cbJob(Job job) {

        mJobMap.put(job.logicalId,job.jobName);
        mVSubInfo.add(job);
        mDagInfo.addNewJob( job );

        //check for compound executables
        if( this.mCompoundTransformations.containsKey( job.getCompleteTCName() ) ){
            CompoundTransformation ct = this.mCompoundTransformations.get( job.getCompleteTCName() );
            //add all the dependant executables and data files
            for( PegasusFile pf : ct.getDependantFiles() ){
                job.addInputFile( pf );
                String lfn = pf.getLFN();
                mDagInfo.updateLFNMap(lfn,"i");
            }
        }

        //put the input files in the map
        for ( Iterator it = job.inputFiles.iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            String lfn = pf.getLFN();
            mDagInfo.updateLFNMap(lfn,"i");
        }

        for ( Iterator it = job.outputFiles.iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            String lfn = ( pf ).getLFN();

            //if the output LFN is also an input LFN of the same
            //job then it is a pass through LFN. Should be tagged
            //as i only, as we want it staged in

            if( job.inputFiles.contains( pf ) ){
                //dont add to lfn map in DagInfo
                continue;
            }
            mDagInfo.updateLFNMap(lfn,"o");
        }


    }

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, List<PCRelation> parents) {
        PCRelation relation;
        child  = (String)mJobMap.get(child);
        String parent;

        for ( PCRelation pc : parents  ){
//            parent = (String)mJobMap.get((String)it.next());
            parent = (String)mJobMap.get( pc.getParent() );
            if(parent == null){
                //this actually means dax is generated wrong.
                //probably some one tinkered with it by hand.
                throw new RuntimeException( "Cannot find parent for job " + child );
            }
            mDagInfo.addNewRelation(parent,child);
        }

    }

    /**
     * Callback when the parsing of the document is done. It sets the flag
     * that the parsing has been done, that is used to determine whether the
     * ADag object has been fully generated or not.
     */
    public void cbDone() {
        mDone = true;
    }

    /**
     * Returns an ADag object corresponding to the abstract plan it has generated.
     * It throws a runtime exception if the method is called before the object
     * has been created fully.
     *
     * @return  ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject(){
        if(!mDone)
            throw new RuntimeException("Method called before the abstract dag " +
                                       " for the partition was fully generated");




        return new ADag(mDagInfo,mVSubInfo);
    }

    /**
     * Callback when a compound transformation is encountered in the DAX
     *
     * @param compoundTransformation   the compound transforamtion
     */
    public void cbCompoundTransformation( CompoundTransformation compoundTransformation ){
        this.mCompoundTransformations.put( compoundTransformation.getCompleteName(), compoundTransformation );
    }

    /**
     * Callback when a replica catalog entry is encountered in the DAX
     *
     * @param rl  the ReplicaLocation object
     */
    public void cbFile( ReplicaLocation rl ){
        this.mReplicaStore.add( rl );
    }

    /**
     * Callback when a transformation catalog entry is encountered in the DAX
     *
     * @param tce  the transformationc catalog entry object.
     */
    public void cbExecutable( TransformationCatalogEntry tce ){
        this.mTransformationStore.addEntry( tce );
    }
}
