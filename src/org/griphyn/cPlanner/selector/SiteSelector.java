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
package org.griphyn.cPlanner.selector;



import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.common.util.DynamicLoader;

import org.griphyn.common.catalog.transformation.Mapper;

import java.util.Iterator;
import java.util.List;

/**
 * The interface that supports callout to a separate site selector, that could be
 * java based or an arbitary executable. Any class interfacing with or implementing
 * a site scheduler should implement this interface.
 * An implementation is provided that calls out to an executable, instead of
 * making an api call to a java based site selector.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gaurang Mehta
 *
 *
 * @version $Revision$
 */
public abstract class SiteSelector {

    /**
     * The name of the package where the implementing classes reside.
     */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.selector.site";

    /**
     * The value for the pool handle, when the pool is not found.
     */
    public static final String POOL_NOT_FOUND = "NONE";

    /**
     * The path to the executable representing the site selector that need to be
     * called. In case the site selector is implemented in the implementing class
     * or interfaced as an api call, the path can be set to null.
     */
    protected String mSiteSelectorPath;

    /**
     * Handle to the TCMapper.
     */
    protected Mapper mTCMapper;

    /**
     * The reference to the ADag object containing the workflow, whose jobs are
     * being sent to the site selector to be scheduled. It contains extra
     * metadata about the workflow like label,timestamp etc that might be used
     * by some site selectors.
     */
    protected ADag mAbstractDag;


    /**
     * The default constructor.
     */
    public SiteSelector(){
        mSiteSelectorPath = null;
        mAbstractDag      = null;
        mTCMapper          = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param path   the path to the site selector. It can be null.
     */
    public SiteSelector(String path) {
        mSiteSelectorPath = path;
        mAbstractDag      = null;
        mTCMapper         = null;
    }

    /**
     * It sets the abstract dag whose jobs are to be scheduled by the site
     * selector.
     *
     * @param dag  the <code>ADag</code> object containing the abstract workflow
     *             that needs to be mapped.
     */
    public void setAbstractDag(ADag dag){
        mAbstractDag = dag;
    }

    /**
     * It sets the tcmapper which will generate a valid map for a given executable.
     * @param mapper Mapper
     */
    public void setTCMapper(Mapper mapper){
        mTCMapper=mapper;
    }

    /**
     * The call out to the site selector to determine on what pool
     * the job should be scheduled. Any class interfacing with or implementing a
     * site scheduler should implement this function.
     * An implementation is provided that takes the <code>SubInfo<code>
     * object and dumps it into a temporary file, which is provided as input to
     * a separate executable representing the site selector.This executable writes
     * out the selected pool in it's stdout, which is picked up by the implementation
     * and returned to the calling method.
     *
     * @param job SubInfo   the <code>SubInfo</code> object  corresponding to the
     *                  job whose execution pool we want to determine.
     *
     * @param pools     the list of <code>String</code> objects representing the
     *                  execution pools that can be used.
     *
     * @return if the pool is found to which the job can be mapped, a string of the
     *         form <code>executionpool:jobmanager</code> where the jobmanager can
     *          be null. If the pool is not found, then set poolhandle to NONE.
     *          null - if some error occured .
     */
    public abstract String mapJob2ExecPool(SubInfo job, List pools);

    /**
     * This method returns a String describing the site selection technique
     * that is being implemented by the implementing class.
     * @return String
     */
    public abstract String description();

    /**
     * Loads the implementing class corresponding to the mode specified by the user
     * at runtime in the properties file.
     *
     * @param className  The name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package. That
     *                   is added by itself.
     * @param path       path to the external site selector.
     * @return SiteSelector
     */
    public static SiteSelector loadSiteSelector(String className,String path) {

        //prepend the package name
        className = PACKAGE_NAME + "." + className;

        //try loading the class dynamically
        SiteSelector ss = null;
        DynamicLoader dl = new DynamicLoader( className);
        try {
               Object argList[] = new Object[1];
               argList[0] = (path == null) ? new String():path;
               ss = (SiteSelector) dl.instantiate(argList);
        }
        catch ( Exception e ) {
            throw new RuntimeException( dl.convertException(e) );
        }

        return ss;
    }


    /**
     * The call out to map a list of jobs on to the execution pools. A default
     * implementation is provided that internally calls mapJob2ExecPool(SubInfo,
     * String,String,String) to map each of the jobs sequentially to an execution pool.
     * The reason for this method is to support site selectors that
     * make their decision on a group of jobs i.e use backtracking to reach a good
     * decision.
     * The implementation that calls out to an executable using Runtime does not
     * implement this method, but relies on the default implementation defined
     * here.
     *
     * @param jobs      the list of <code>SubInfo</code> objects representing the
     *                  jobs that are to be scheduled.
     * @param pools     the list of <code>String</code> objects representing the
     *                  execution pools that can be used.
     *
     * @return an Array of String objects, corresponding to the jobs list and each
     *         String of the form <code>executionpool:jobmanager</code> where the
     *         jobmanager can be null.
     *         null - if some error occured .
     *
     */
    public String[] mapJob2ExecPool(List jobs, List pools){
          //traverse through the list and allocate each job
          //at a time
          Iterator it = jobs.iterator();
          SubInfo sub;
          String res[] = new String[jobs.size()];
          String pool  = null;
          String jm    = null;

          int i = 0;
          while(it.hasNext()){
              sub = (SubInfo)it.next();
              res[i] = this.mapJob2ExecPool(sub,pools);
              i++;

          }

          return res;
    }

}
