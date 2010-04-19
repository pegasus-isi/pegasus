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

package org.griphyn.cPlanner.transfer.refiner;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.FactoryException;

/**
 * This transfer refiner incorporates chaining for the impelementations that
 * can transfer only one file per transfer job, by delegating it to the Chain
 * refiner implementation.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public class SChain extends SDefault {

    /**
     * The handle to the chain refiner.
     */
    protected Chain mChainRefiner;


    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION =
        "SChain Mode (the stage in jobs being chained together in bundles";

    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param bag        the bag of initialization objects
     *
     */
    public SChain( ADag dag, PegasusBag bag ){
        super( dag, bag );
        mChainRefiner = new Chain( dag, bag );
	try{
            //hmm we are bypassing the factory check!
            mChainRefiner.loadImplementations( bag );
        }
        catch(Exception e){
            throw new FactoryException( "While loading in SChain " , e );

        }
    }

    /**
     * Adds a new relation to the workflow. In the case when the parent is a
     * transfer job that is added, the parentNew should be set only the first
     * time a relation is added. For subsequent compute jobs that maybe
     * dependant on this, it needs to be set to false.
     *
     * @param parent    the jobname of the parent node of the edge.
     * @param child     the jobname of the child node of the edge.
     * @param site      the execution site where the transfer node is to be run.
     * @param parentNew the parent node being added, is the new transfer job
     *                  and is being called for the first time.
     */
    public void addRelation(String parent,
                            String child,
                            String site,
                            boolean parentNew){
        //delegate to the Chain refiner
        mChainRefiner.addRelation(parent,child,site,parentNew);
    }


    /**
     * Prints out the bundles and chains that have been constructed.
     */
    public void done(){
        //delegate to the Chain refiner
        mChainRefiner.done();
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

}
