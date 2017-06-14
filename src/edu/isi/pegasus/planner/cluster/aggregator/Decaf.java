/**
 *  Copyright 2007-2017 University Of Southern California
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
package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import java.util.List;

/**
 * Decaf data flows are represented as clustered job. DECAF implementation to
 * render the data flow in the DECAF JSON.
 *
 * @author Karan Vahi
 */
public class Decaf implements JobAggregator{

    public void initialize(ADag dag, PegasusBag bag) {
        
    }

   
    /**
     * Enables the abstract clustered job for execution and converts it to it's 
     * executable form
     * 
     * @param job          the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete( AggregatedJob job ){
        throw new UnsupportedOperationException("Not supported yet.");  
    }
   
    
    /**
     * The aggregated job is already constructed during parsing of the DAX.
     * Not implemented.
     * 
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     * @return  the <code>SubInfo</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob(List jobs,String name,String id){
        throw new UnsupportedOperationException("Not supported yet.");  
    }

    /**
     * A boolean indicating whether ordering is important while traversing 
     * through the aggregated job.
     * 
     * @return a boolean
     */
    public boolean topologicalOrderingRequired(){
        //we don't care about ordering, and decaf jobs can have cycles
        return false;
    }
    
    
    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     */
    public String getClusterExecutableLFN(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    
    /**
     * Returns the executable basename of the clustering executable used.
     * 
     * @return the executable basename.
     */
    public String getClusterExecutableBasename(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
