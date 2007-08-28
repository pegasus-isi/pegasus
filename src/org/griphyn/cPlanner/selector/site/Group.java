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
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

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

public class Group extends Abstract {


    /**
     * The description of the site selector.
     */
    private static final String mDescription =
        "Site selector doing clustering on the basis of key group in pegasus namespace";

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
     * The handle to the internal site selector that is used to schedule jobs
     * amongst the groups.
     */
    private AbstractPerJob mSelector;



    /**
     * The default constructor.
     */
    public Group() {
        mGroupMap = new TreeMap();
        mSelector = new Random();
        mLogger   = LogManager.getInstance();
    }

    /**
     * Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( PegasusBag bag ){
        super.initialize( bag );
        mSelector.initialize( bag );
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
     * String,String,String) to map each of the jobs sequentially to an execution site.
     * The reason for this method is to support site selectors that
     * make their decision on a group of jobs i.e use backtracking to reach a good
     * decision.
     * The implementation that calls out to an executable using Runtime does not
     * implement this method, but relies on the default implementation defined
     * here.
     *
     * @param workflow  the workflow that needs to be scheduled.
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution pools that can be used.
     *
     */
    public void mapWorkflow( Graph workflow, List sites) {
          SubInfo job;
          List l = null;

          int i = 0;
          for(Iterator it = workflow.nodeIterator();it.hasNext(); ){
              GraphNode node = ( GraphNode )it.next();
              job = ( SubInfo )node.getContent();
              //put the jobs into the map grouped by key VDS_GROUP_KEY
              insert(job);
          }

          //traverse through the group map and send off the first job
          //in each group to the internal site selector.
          for(Iterator it = mGroupMap.entrySet().iterator();it.hasNext();){
              Map.Entry entry = (Map.Entry)it.next();
              boolean defaultGroup = entry.getKey().equals( mDefaultGroup );
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
              mSelector.mapJob( job, sites );

              //traverse thru the remaining jobs in the group
              for(Iterator it1 = l.iterator();it1.hasNext();){
                  SubInfo j = (SubInfo)it1.next();
                  if ( defaultGroup ){
                      //each job in the group has to be
                      //mapped individually
                      mSelector.mapJob( j, sites);
                  }
                  else{
                      //mapping same as the one for
                      //for the first job in group
                      j.setSiteHandle( job.getSiteHandle() );
                  }
              }
          }
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
