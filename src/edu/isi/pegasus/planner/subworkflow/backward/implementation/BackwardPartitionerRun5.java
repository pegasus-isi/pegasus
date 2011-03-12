/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow.backward.implementation;

import edu.isi.pegasus.planner.subworkflow.backward.BackwardPartitioner;
import edu.isi.pegasus.planner.subworkflow.backward.DAXAbstraction;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.dax.ADAG;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 * @author wchen
 */
public class BackwardPartitionerRun5 extends BackwardPartitioner{
    
    
    public BackwardPartitionerRun5(ADag mDag, PegasusBag mBag)
    {
        super(mDag, mBag);
    }

    protected ArrayList getSelfChildren(ArrayList jobList, String jobID)
    {
        jobList.add(jobID2jobLogicalID(jobID));

        for(Iterator it = mDag.getChildren(jobID).iterator();it.hasNext();)
        {
            String jobname = (String )it.next();
            //edu.isi.pegasus.planner.classes.Job parent = getDagJob(jobname);
            String logicalID = jobID2jobLogicalID(jobname);
            String color = (String)mJobColor.get(logicalID);
            if((color==null)|| (!color.equals("BLACK")&& !color.equals("BLACK")))
            {
                if(!jobList.contains(jobname))
                {
                        //jobList.add(jobname);
                        jobList = getSelfChildren(jobList, jobname);
                }

            }
        }
        return jobList;
    }
    @Override
    public void run()
    {
        getProperties();
        ADAG dax = new ADAG("subworkflow" + mDaxMap.size());
        DAXAbstraction bigDax = new DAXAbstraction(dax);
        Map fList= new HashMap<String, Double>();
        mDaxMap.put(bigDax, fList);
        int siteIndex = 0;
        LinkedList siteList = new LinkedList<DAXAbstraction>();
        siteList.add(bigDax);
        mIndexSiteList.put(siteIndex, siteList);


        edu.isi.pegasus.planner.classes.Job rear = getRear();
        translate();
        mQueue.clear();
        mQueue.addLast( rear );
        int i = 0;
        SiteCatalogEntry se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
        double limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;
        bigDax.limit = limit;

        while( !mQueue.isEmpty() ){

/**There is a problem, it only works for fan-in and fan-out struct.
 * Pay attention to a parent has two children and one has reached
 **/

            edu.isi.pegasus.planner.classes.Job dagJob =
                    (edu.isi.pegasus.planner.classes.Job)mQueue.getLast();

            String ccr = (String)mJobColor.get(dagJob.logicalId);
            if((ccr != null) && ccr.equals("BLACK"))
            {
                mQueue.remove(dagJob);
                continue;
            }
            if(!depthMap.containsKey(dagJob.jobID))
            {
                i = 0;
                depthMap.put(dagJob.jobID, 0);
            }
            else
            {
                i = (Integer)depthMap.get(dagJob.jobID);
            }

                ArrayList jobList = new ArrayList();

                if(mDag.getParents(dagJob.jobID).size() > 10 )
                {
                    if(true)
                        //size limit > limit
                    {
                        updateSizeMap(bigDax, dagJob);
                        addDAGJob(dagJob, dax);
                        mJob2DAX.put(dagJob.getLogicalID(),dax);
                        mJobColor.put(dagJob.logicalId, "BLACK");

                        dax = new ADAG("subworkflow" + mDaxMap.size());
                        bigDax = new DAXAbstraction(dax);
                        fList = new HashMap<String, Double>();
                        mDaxMap.put(bigDax, fList);
                        se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
                        limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;
                        bigDax.limit = limit;

                        siteList = (LinkedList)mIndexSiteList.get(siteIndex);
                        siteList.add(bigDax);
                        jobList = new ArrayList();
                        //jobList.add(jobID2jobLogicalID(dagJob.jobID));


                        //do no site Index ++ which means they still use the same site possibly
                        //siteIndex ++;
                    }
                    else
                        // please add it later.
                    {

                    }

                }
                else
                {
                    jobList = getSelfChildren(jobList, dagJob.jobID);
                    // This should be changed
                    double sizeA = getSizeMap(bigDax);
                    double sizeB = getSizeMap(jobList, siteIndex);
                    double sizeC = sizeA + sizeB;
                    if( (sizeC) > limit )
                    {
                        //printout(dax);
                        //System.out.println("The size of dax is " + getSizeMap(bigDax) + " + " + getSizeMap(bigDax,jobList));
                        System.out.println("the breakpoint is "+dagJob.jobID);
                        dax = new ADAG("subworkflow" + mDaxMap.size());
                        bigDax = new DAXAbstraction(dax);
                        fList = new HashMap<String, Double>();
                        mDaxMap.put(bigDax, fList);
                        se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
                        //System.out.println(se.getSiteHandle() + "'s size limit");
                        limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;
                        bigDax.limit = limit;
                        siteList = new LinkedList<DAXAbstraction>();
                        siteList.add(bigDax);
                        siteIndex ++;
                        mIndexSiteList.put(siteIndex, siteList);
                    }



                    for(Iterator it = jobList.iterator();it.hasNext();)
                    {
                        edu.isi.pegasus.planner.classes.Job job = getDagJob((String)it.next());
                        updateSizeMap(bigDax, job);
                        addDAGJob(job, dax);
                        mJob2DAX.put(job.getLogicalID(),dax);
                        mJobColor.put(job.logicalId, "BLACK");

                    }
                }

                //xxxxxx wrong here all parent

                for(Iterator it = mDag.getParents(dagJob.jobID).iterator();it.hasNext();)
                {


                    String jobname = (String )it.next();
                    edu.isi.pegasus.planner.classes.Job parent = getDagJob(jobname);
                    String color ="";

                    if(!mJobColor.containsKey(parent.logicalId))
                    {
                        mJobColor.put(parent.logicalId, "WHITE");
                        color = "WHITE";
                    }
                    else
                         color = (String)mJobColor.get(parent.logicalId);

                    if(!color.equals("GRAY") && !color.equals("BLACK"))
                    {
                        mQueue.addLast(parent);
                        mJobColor.put(parent.logicalId, "GRAY");
                        depthMap.put(parent.jobID, i+1);

                    }
                }
                mJobColor.put(dagJob.logicalId, "BLACK");
                mQueue.remove(dagJob);
                //System.out.println("Job " + dagJob.logicalId +" is done");
            }
        System.out.println("Jobs are all done");


        super.run();

    }




}
