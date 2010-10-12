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
package edu.isi.pegasus.planner.cluster;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.PegasusFile;

import org.griphyn.cPlanner.partitioner.Topological;
import org.griphyn.cPlanner.partitioner.Partition;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * The vertical cluster, that extends the Default clusterer and topologically
 * sorts the partition before clustering the jobs into aggregated jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Vertical extends Abstract {

    /**
     * A short description about the partitioner.
     */
    public static final String DESCRIPTION = "Topological based Vertical Clustering";


    /**
     * The default constructor.
     */
    public Vertical(){
        super();
    }


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description(){
        return this.DESCRIPTION;
    }


    /**
     * Returns the nodes in the partition as a List in the topologically sorted
     * order.
     *
     * @param p  the partition whose nodes have to be ordered.
     *
     * @return an ordered List of <code>String</code> objects that are the ID's
     *         of the nodes.
     *
     * @throws ClustererException in case of error.
     */
    public List order( Partition p ) throws ClustererException{
        try{
            return new Topological(p).sort();
        }
        catch( Exception e){
            throw new ClustererException( "Unable to sort the partition " + p.getID(),
                                          e );
        }
    }

    /**
     * Determine the input and output files of the job on the basis of the
     * order of the constituent jobs in the AggregatedJob.
     * The input and output files are determined on the basis of topologically
     * sorted order of the constituent jobs.
     *
     * @param job  the <code>AggregatedJob</code>
     *
     * @throws ClustererException in case of error.
     */
    public void determineInputOutputFiles( AggregatedJob job ){

        //set the input files to null for time being
        job.inputFiles  = null;
        job.outputFiles = null;

        Set inputFiles        = new HashSet();
        Set materializedFiles = new HashSet();

        PegasusFile file;

        //the constituent jobs are topologically sorted
        //traverse through them and build up the ip and op files
        for( Iterator it = job.constituentJobsIterator(); it.hasNext();){
            SubInfo cjob = (SubInfo)it.next();

            //traverse through input files of constituent job
            for( Iterator fileIt = cjob.getInputFiles().iterator(); fileIt.hasNext();){
                file = (PegasusFile)fileIt.next();
                //add to input files if it has not been materializd
                if( !materializedFiles.contains( file ) ){ inputFiles.add( file ); }
            }

            //traverse through output files of constituent job
            for( Iterator fileIt = cjob.getOutputFiles().iterator(); fileIt.hasNext();){
                file = (PegasusFile)fileIt.next();
                //add to materialized files
                materializedFiles.add( file );

//                //file is output only if it has to be registered or transferred
//                if ( !file.getTransientRegFlag() ||
//                     file.getTransferFlag() != PegasusFile.TRANSFER_NOT ){
//                    outputFiles.add( file );
//                }
            }
        }

        job.setInputFiles( inputFiles );

        //all the materialized files are output files for
        //the aggregated job.
        job.setOutputFiles( materializedFiles );

    }



}
