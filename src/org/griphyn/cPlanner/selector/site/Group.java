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

package org.griphyn.cPlanner.selector.site;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.selector.SiteSelector;

import org.griphyn.common.catalog.transformation.Mapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A site selector than ends up doing grouping jobs together on the basis of
 * an identifier specifed in the dax for the jobs, and schedules them on to the
 * same site. Currently, the identifier is key <code>group</code> in the vds
 * profile namespace. All the jobs that do not have a group associated with them
 * are put in one default group and end up being scheduled on the same pool.
 * A limitation of this site selector is that it does not check whether all the
 * jobs can be scheduled on a particular pool or not. It just checks whether
 * the first job can be or not. The reason for that is after the grouping the
 * the selector just hands the first job in each group to the other site selectors
 * that work on jobs. Currently, it hands it to the Random Site Selector.
 *
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @author Mei-Hui Su
 *
 * @version $Revision$
 */

public class Group extends SiteSelector {


    /**
     * The description of the site selector.
     */
    private static final String mDescription =
        "Site selector doing clustering on the basis of key group in vds namespace";

    /**
     * The name of the group into which jobs are grouped if no group is
     * specified in the dax.
     */
    private static final String mDefaultGroup = "default";

    /**
     * The map containing the the jobs grouped by the key group.
     */
    private Map mGroupMap;

    /**
     * An index map containing the index of the jobs in the list passed with
     * the name of the job.
     */
    private Map mIndexMap;

    /**
     * The handle to the internal site selector that is used to schedule jobs
     * amongst the groups.
     */
    private SiteSelector mSelector;

    /**
     * The handle to the logger.
     */
    private LogManager mLogger;

    /**
     * The default constructor.
     */
    public Group() {
        mGroupMap = new TreeMap();
        mIndexMap = new HashMap();
        mSelector = new Random();
        mLogger   = LogManager.getInstance();
    }

    /**
     * The overloaded constructor.
     * The path is null in this case.
     *
     * @param path  the path to the site selector.
     */
    public Group(String path) {
        super(path);
        mGroupMap = new TreeMap();
        mIndexMap = new HashMap();
        mSelector = new Random(path);
        mLogger   = LogManager.getInstance();
    }

    /**
     * Returns the description of the site selector.
     *
     * @return description.
     */
    public String description() {
        return mDescription;
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
          SubInfo job;
          String res[] = new String[jobs.size()];
          String pool  = null;
          String jm    = null;
          List l = null;

          int i = 0;
          for(Iterator it = jobs.iterator();it.hasNext(); i++){
              job = (SubInfo)it.next();
              //put the jobs into the map grouped by key VDS_GROUP_KEY
              insert(job);
              //associate in the index map the jobname with the index
              mIndexMap.put(job.jobName,new Integer(i));
          }

          //traverse through the group map and send off the first job
          //in each group to the internal site selector.
          for(Iterator it = mGroupMap.entrySet().iterator();it.hasNext();){
              Map.Entry entry = (Map.Entry)it.next();
              boolean defaultGroup = entry.getKey().equals(mDefaultGroup);
              mLogger.log("[Group Selector]Mapping jobs in group " + entry.getKey(),
                          LogManager.DEBUG_MESSAGE_LEVEL);

              l = (List)entry.getValue();
              String msg = "\t{";
              boolean first = true;
              for(Iterator it1 = l.iterator();it1.hasNext();){
                  msg += (first)? "" : ",";
                  msg += ((SubInfo)it1.next()).jobName ;
                  first = false;
              }
              msg += "}";
              mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
              //hand of the first job to the internal selector
              job = (SubInfo)l.get(0);
              res[getIndex(job)] = mapJob2ExecPool(job,pools);

              //traverse thru the remaining jobs in the group
              for(Iterator it1 = l.iterator();it1.hasNext();){
                  SubInfo j = (SubInfo)it1.next();
                  res[getIndex(j)] = (defaultGroup)?
                                     //each job in the group has to be
                                     //mapped individually
                                     mapJob2ExecPool(j,pools):
                                     //mapping same as the one for
                                     //for the first job in group
                                     res[getIndex(job)];
              }
          }

          //res[i] = this.mapJob2ExecPool(job,pools);

          return res;
    }

    /**
     * Maps the job using the internal site selector. Hands off to the internal
     * site selector and returns it's result!
     */
    public String mapJob2ExecPool(SubInfo job, List pools) {
        return mSelector.mapJob2ExecPool(job,pools);
    }

    /**
     * Sets the tcmapper which will generate a valid map for a given executable.
     *
     * @param mapper Mapper containing the various mappings.
     */
    public void setTCMapper(Mapper mapper){
        super.setTCMapper(mapper);
        //in addition set the mapper for the
        //internal site selector
        mSelector.setTCMapper(mapper);
    }


    /**
     * Returns the index of the job.
     *
     * @param job the job whose index is reqd.
     *
     * @return int
     */
    private int getIndex(SubInfo job){
        //no checks as there is a guarentee of it being in the map
        return ((Integer)mIndexMap.get(job.jobName)).intValue();
    }

    /**
     * Inserts the job into the group map.
     *
     * @param job  the job to be inserted.
     */
    private void insert(SubInfo job){
        Object obj = job.vdsNS.get(VDS.GROUP_KEY);
        if(obj != null && ((String)obj).equalsIgnoreCase(mDefaultGroup)){
            //throw an exception?
            throw new RuntimeException( "The group name " + mDefaultGroup +
                        " is a reserved keyword for the selector." +
                        " Use another group name in your DAX" );
        }
        String key = (obj == null)?
                     //no group specified. set to default
                     mDefaultGroup:
                     //get the value from the profile
                     (String)obj;

        if(mGroupMap.containsKey(key)){
            //there is already a group associated.
            List l = (List)mGroupMap.get(key);
            l.add(job);
        }
        else{
            //insert a new entry to the map
            List l = new LinkedList();
            l.add(job);
            mGroupMap.put(key,l);
        }
    }

}
