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

package org.griphyn.cPlanner.parser.dax;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.PCRelation;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.SubInfo;

import edu.isi.pegasus.common.logging.LogManager;
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
     * Contains SubInfo objects. One per submit file.
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
     * @param job  the <code>SubInfo</code> object storing the job information
     *             gotten from parser.
     */
    public void cbJob(SubInfo job) {
        mJobMap.put(job.logicalId,job.jobName);
        mVSubInfo.add(job);
        mDagInfo.addNewJob( job );

        //put the input files in the map
        for ( Iterator it = job.inputFiles.iterator(); it.hasNext(); ){
            String lfn = ((PegasusFile)it.next()).getLFN();
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
    public void cbParents(String child, List parents) {
        PCRelation relation;

        child  = (String)mJobMap.get(child);
        String parent;

        for ( Iterator it = parents.iterator(); it.hasNext(); ){
            parent = (String)mJobMap.get((String)it.next());
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
}
