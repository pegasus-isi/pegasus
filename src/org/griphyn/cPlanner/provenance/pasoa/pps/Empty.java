/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.provenance.pasoa.pps;

import java.util.*;

import org.griphyn.cPlanner.engine.Refiner;

import org.griphyn.cPlanner.provenance.pasoa.PPS;

/**
 * The default empty implementation to be used.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Empty implements PPS {

    public Empty() {
    }

    /**
     *
     * @return The ID used for the whole refinement process of this workflow
     * @param refiner  workflow Refiner
     * @param refinementStepName String
     * @param firstStep boolean
     * @throws Exception
     */
    public String beginWorkflowRefinementStep( Refiner refiner,
                                               String refinementStepName,
                                               boolean firstStep ) throws
        Exception {

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
    public void clusteringOf( String clusteredJob, List jobs ) throws Exception {
//        System.out.println( "Clustered Job " + clusteredJob );
//        System.out.println( " contains " + jobs );

    }

    /**
     * endWorkflowRefinementStep
     *
     * @param refiner  workflow Refiner
     * @throws Exception
     */
    public void endWorkflowRefinementStep( Refiner refiner ) throws Exception {

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
    public void isIdenticalTo(String afterNode, String beforeNode) throws
        Exception {
//        System.out.println( beforeNode + " identical to " + afterNode );
    }

    /**
     * isPartitionOf
     *
     * @param afterNode String
     * @param beforeNode List
     * @throws Exception
     */
    public void isPartitionOf(String afterNode, List beforeNode) throws
        Exception {
    }

    /**
     * registrationIntroducedFor
     *
     * @param registrationNode String
     * @param dataStagingNode String
     * @throws Exception
     */
    public void registrationIntroducedFor( String registrationNode,
                                           String dataStagingNode ) throws
        Exception {
//        System.out.println( "registration node " + registrationNode + " for " + dataStagingNode );
    }

    /**
     * siteSelectionFor
     *
     * @param afterNode String
     * @param beforeNode String
     * @throws Exception
     */
    public void siteSelectionFor(String afterNode, String beforeNode) throws
        Exception {
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
    public void stagingIntroducedFor(List stagingNodes, String appNode) throws
        Exception {
//        System.out.println( "Staging done by " + stagingNodes + " for " + appNode);
    }
}
