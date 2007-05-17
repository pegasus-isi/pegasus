/*
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
package org.griphyn.cPlanner.classes;

/**
 * This is a data class that stores the contents of the transfer job that
 * transfers the data. Later on stdin etc, would be stored in it.
 *
 * @author Karan Vahi vahi@isi.edu
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision: 1.1 $
 */
public class TransferJob extends SubInfo {

    /**
     * The site at which the transfer jobs would have run, had it been running
     * in a peer 2 peer transfer mode (non third party mode).
     */
    private String mNonTPTSite;

    /**
     * The default constructor.
     */
    public TransferJob() {
        super();
        mNonTPTSite = null;
    }

    /**
     * The overloaded construct that constructs a GRMS job by wrapping around
     * the <code>SubInfo</code> job.
     *
     * @param job  the original job description.
     */
    public TransferJob(SubInfo job){
        super(job);
        mNonTPTSite = null;
    }

    /**
     * Returns the site at which the job would have run if the transfer job was
     * being run in non third party mode. If the job is run in a non third party
     * mode, the result should be the same as the site where the transfer job
     * has been scheduled.
     *
     * @return the site at which the job would have run in a non third party mode,
     *         null if not set.
     */
    public String getNonThirdPartySite(){
        return mNonTPTSite;
    }

    /**
     * Sets the non third party site for the transfer job. This is the site
     * at which the job would have run if the transfer job was being run in
     * non third party mode.
     *
     * @param site the name of the site
     */
    public void setNonThirdPartySite(String site){
        mNonTPTSite = site;
    }

    /**
     * Returns a textual description of the Transfer Job.
     *
     * @return the textual description.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer(super.toString());
        sb.append("\n").append(" Non TPT Site     :").
        append(getNonThirdPartySite());

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
        TransferJob newJob = new TransferJob((SubInfo)super.clone());
        newJob.setNonThirdPartySite(this.getNonThirdPartySite());
        return newJob;
    }


}