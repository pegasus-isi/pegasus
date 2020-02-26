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
package edu.isi.pegasus.planner.partitioner;

import edu.isi.pegasus.common.logging.LogManager;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dax.Filename;
import org.griphyn.vdl.dax.Job;
import org.griphyn.vdl.euryale.Callback;

/**
 * This class ends up writing a partitioned dax, that corresponds to one partition as defined by the
 * Partitioner. Whenever it is called to write out a dax corresponding to a partition it looks up
 * the dax i.e parses the dax and gets the information about the jobs making up the partition.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class MultipleLook extends DAXWriter {

    /** The set of job id's in the partition. */
    private Set mNodeSet;

    /** A map containing the relations between the jobs making up the partition. */
    private Map mRelationsMap;

    /** The ADAG object containing the partitioned dax. */
    private ADAG mPartADAG;

    /** The number of jobs that are in the partition. */
    private int mNumOfJobs;

    /** The number of jobs about which the callback interface has knowledge. */
    private int mCurrentNum;

    /** The index of the partition that is being written out. */
    private int mIndex;

    /**
     * The overloaded constructor.
     *
     * @param daxFile the path to the dax file that is being partitioned.
     * @param directory the directory in which the partitioned daxes are to be generated.
     */
    public MultipleLook(String daxFile, String directory) {
        super(daxFile, directory);
        mIndex = -1;
    }

    /**
     * It writes out a dax consisting of the jobs as specified in the partition.
     *
     * @param partition the partition object containing the relations and id's of the jobs making up
     *     the partition.
     * @return boolean true if dax successfully generated and written. false in case of error.
     */
    public boolean writePartitionDax(Partition partition, int index) {

        // do the cleanup
        mPartADAG = null;
        mNodeSet = null;
        mRelationsMap = null;
        mIndex = index;

        // get from the partition object the set of jobs
        // and relations between them
        mNodeSet = partition.getNodeIDs();
        mRelationsMap = partition.getRelations();
        mNumOfJobs = mNodeSet.size();

        // set the current number of jobs whose information we have
        mCurrentNum = 0;

        mPartADAG = new ADAG(0, index, mPartitionName);

        Callback callback = new MyCallBackHandler();
        org.griphyn.vdl.euryale.DAXParser d = new org.griphyn.vdl.euryale.DAXParser(null);
        d.setCallback(callback);
        d.parse(mDaxFile);

        // do the actual writing to the file
        this.initializeWriteHandle(mIndex);
        try {
            mPartADAG.toXML(mWriteHandle, "");
        } catch (IOException e) {
            mLogger.log(
                    "Error while writing out a partition dax :" + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        this.close();

        return true;
    }

    /**
     * The internal callback handler for the DAXParser in Euryale. It only stores the jobs that are
     * part of the dax, that are then populated into the internal ADAG object that is used to write
     * out the dax file corresponding to the partition.
     */
    private class MyCallBackHandler implements Callback {

        /** The empty constructor. */
        public MyCallBackHandler() {}

        /**
         * Callback when the opening tag was parsed. The attribute maps each attribute to its raw
         * value. The callback initializes the DAG writer.
         *
         * @param attributes is a map of attribute key to attribute value
         */
        public void cb_document(Map attributes) {
            // do nothing at the moment
        }

        public void cb_filename(Filename filename) {}

        /** */
        public void cb_job(Job job) {
            List fileList = null;
            Iterator it;

            if (mNodeSet.contains(job.getID())) {
                mCurrentNum++;
                mPartADAG.addJob(job);
                fileList = job.getUsesList();

                // iterate through the file list
                // populate it in the ADAG object
                it = fileList.iterator();
                while (it.hasNext()) {
                    Filename file = (Filename) it.next();
                    mPartADAG.addFilename(
                            file.getFilename(),
                            (file.getLink() == LFN.INPUT) ? true : false,
                            file.getTemporary(),
                            file.getDontRegister(),
                            file.getDontTransfer());
                }
            }
        }

        public void cb_parents(String child, List parents) {}

        public void cb_done() {
            List parentIDs;
            // print the xml generated so far

            if (mCurrentNum != mNumOfJobs) {
                // throw an error and exit.
                throw new RuntimeException(
                        "Could not find information about all the jobs"
                                + " in the dax for partition "
                                + mNodeSet);
            }

            // add the relations between the jobs in the partition to the ADAG
            Iterator it = mRelationsMap.keySet().iterator();
            while (it.hasNext()) {
                String childID = (String) it.next();
                parentIDs = (List) mRelationsMap.get(childID);

                // get all the parents of the children and populate them in the
                // ADAG object
                Iterator it1 = parentIDs.iterator();
                while (it1.hasNext()) {
                    mPartADAG.addChild(childID, (String) it1.next());
                }
            }
        }
    }
}
