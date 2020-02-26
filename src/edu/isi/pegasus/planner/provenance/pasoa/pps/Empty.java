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

import edu.isi.pegasus.planner.provenance.pasoa.PPS;
import edu.isi.pegasus.planner.refiner.Refiner;
import java.util.*;

/**
 * The default empty implementation to be used.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Empty implements PPS {

    public Empty() {}

    /**
     * @return The ID used for the whole refinement process of this workflow
     * @param refiner workflow Refiner
     * @param refinementStepName String
     * @param firstStep boolean
     * @throws Exception
     */
    public String beginWorkflowRefinementStep(
            Refiner refiner, String refinementStepName, boolean firstStep) throws Exception {

        //        System.out.println( "Start of Refiner- " + refinementStepName );
        //        System.out.println( "First Step " + firstStep );
        //        System.out.println( refiner.getXMLProducer().toXML() );

        return "";
    }

    /**
     * clusteringOf
     *
     * @param clusteredJob String
     * @param jobs List
     * @throws Exception
     */
    public void clusteringOf(String clusteredJob, List jobs) throws Exception {
        //        System.out.println( "Clustered Job " + clusteredJob );
        //        System.out.println( " contains " + jobs );

    }

    /**
     * endWorkflowRefinementStep
     *
     * @param refiner workflow Refiner
     * @throws Exception
     */
    public void endWorkflowRefinementStep(Refiner refiner) throws Exception {

        //        System.out.println( "End of Refiner" );
        //        System.out.println( refiner.getXMLProducer().toXML() );

    }

    /**
     * isIdenticalTo
     *
     * @param afterNode String
     * @param beforeNode String
     * @throws Exception
     */
    public void isIdenticalTo(String afterNode, String beforeNode) throws Exception {
        //        System.out.println( beforeNode + " identical to " + afterNode );
    }

    /**
     * isPartitionOf
     *
     * @param afterNode String
     * @param beforeNode List
     * @throws Exception
     */
    public void isPartitionOf(String afterNode, List beforeNode) throws Exception {}

    /**
     * registrationIntroducedFor
     *
     * @param registrationNode String
     * @param dataStagingNode String
     * @throws Exception
     */
    public void registrationIntroducedFor(String registrationNode, String dataStagingNode)
            throws Exception {
        //        System.out.println( "registration node " + registrationNode + " for " +
        // dataStagingNode );
    }

    /**
     * siteSelectionFor
     *
     * @param afterNode String
     * @param beforeNode String
     * @throws Exception
     */
    public void siteSelectionFor(String afterNode, String beforeNode) throws Exception {
        //        System.out.print( " Site Selection for " + beforeNode );
        //        System.out.println( " is " + afterNode );

    }

    /**
     * stagingIntroducedFor
     *
     * @param stagingNodes List
     * @param appNode String
     * @throws Exception
     */
    public void stagingIntroducedFor(List stagingNodes, String appNode) throws Exception {
        //        System.out.println( "Staging done by " + stagingNodes + " for " + appNode);
    }
}
