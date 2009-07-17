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


package org.griphyn.cPlanner.classes;

/**
 * This is a data class that stores the contents of the DAG job in a DAX conforming
 * to schema 3.0 or higher.
 *
 * @author Karan Vahi
 * 
 * @version $Revision$
 */
public class DAGJob extends SubInfo {

    /**
     * The DAG File that the job refers to.
     */
    private String mDAGFile;

    /**
     * The default constructor.
     */
    public DAGJob() {
        super();
        mDAGFile = null;
    }

    /**
     * The overloaded construct that constructs a GRMS job by wrapping around
     * the <code>SubInfo</code> job.
     *
     * @param job  the original job description.
     */
    public DAGJob(SubInfo job){
        super(job);
        mDAGFile = null;
    }

    /**
     * Returns the DAGFile the job refers to.
     *
     * @return dag file
     */
    public String getDAGFile(){
        return mDAGFile;
    }

    /**
     * Sets the DAG file
     *
     * @param file  the path to the DAG file.
     */
    public void setDAGFile(String file ){
        mDAGFile = file ;
    }

    /**
     * Returns a textual description of the Transfer Job.
     *
     * @return the textual description.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer(super.toString());
       

        return sb.toString();

    }

    /**
     * Returns a new copy of the Object. The implementation is faulty.
     * There is a shallow copy for the profiles. That is the clone retains
     * references to the original object.
     *
     * @return Object
     */
    public Object clone(){
        DAGJob newJob = new DAGJob((SubInfo)super.clone());
        newJob.setDAGFile( this.getDAGFile() );
        return newJob;
    }


}
