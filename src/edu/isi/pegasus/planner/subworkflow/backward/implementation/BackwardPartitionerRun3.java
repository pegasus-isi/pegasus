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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author wchen
 */
public class BackwardPartitionerRun3 extends BackwardPartitioner{

    public BackwardPartitionerRun3(ADag mDag, PegasusBag mBag)
    {
        super(mDag, mBag);
    }

    public void run3()
    {
        getProperties();
        ADAG dax = new ADAG("subworkflow" + mDaxMap.size());
        DAXAbstraction bigDax = new DAXAbstraction(dax);
        Map fList= new HashMap<String, Double>();
        mDaxMap.put(bigDax, fList);
        //from now on should be implemented by sub clasess
        int siteIndex = 0;

        edu.isi.pegasus.planner.classes.Job rear = getRear();

        translate();

        mQueue.clear();
        mQueue.addLast( rear );
        int i = 0;
        int iter = 0;
        int maxIter = mDag.getNoOfJobs();
        int dex = maxIter/40;
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        while( !mQueue.isEmpty() ){


            iter++;
            if(iter %dex == 0)
                System.out.print("<");

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



            SiteCatalogEntry se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
            double limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;
            bigDax.limit = limit;

                if(  (mDag.getChildren(dagJob.jobID).size() >= 2))
                {

                    if(!checkChildColor(dagJob.jobID))
                    {
                        mQueue.remove(dagJob);
                        mJobColor.put(dagJob.logicalId, "WHITE");
                        //iter --;
                        continue;

                    }
                    else
                    {

                    }

                }

            if(!dagJob.jobID.contains("Notify"))
            {

                if ( (getSizeMap(bigDax, dagJob) + getSizeMap(bigDax))  > limit )
                {


                    double a1 = getSizeMap(bigDax,dagJob);
                    double a2 = getSizeMap(bigDax);
                    dax = new ADAG("subworkflow" + mDaxMap.size());
                    bigDax = new DAXAbstraction(dax);
                    fList = new HashMap<String, Double>();
                    mDaxMap.put(bigDax, fList);
                    siteIndex ++;

                }
                updateSizeMap(bigDax, dagJob);
                addDAGJob(dagJob, dax);
                mJob2DAX.put(dagJob.getLogicalID(),dax);


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
            else
            {

                updateSizeMap(bigDax, dagJob);
                addDAGJob(dagJob, dax);
                mJob2DAX.put(dagJob.getLogicalID(),dax);


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

                    if(!parent.jobID.contains("ZipSeis"))
                    {
                        mQueue.addLast(parent);
                        mJobColor.put(parent.logicalId, "GRAY");
                        depthMap.put(parent.jobID, i+1);

                    }
                    else
                    {
                        updateSizeMap(bigDax, parent);
                        addDAGJob(parent,dax);
                        mJob2DAX.put(parent.getLogicalID(), dax);
                        mJobColor.put(parent.logicalId, "BLACK");
                    }
                }
                mJobColor.put(dagJob.logicalId, "BLACK");
                mQueue.remove(dagJob);
            }

        }
        System.out.println("\nAll jobs are done\n");


    }


}
