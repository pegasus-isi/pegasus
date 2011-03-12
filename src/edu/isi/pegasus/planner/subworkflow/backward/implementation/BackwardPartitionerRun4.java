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
import java.util.Map;

/**
 *
 * @author wchen
 */
public class BackwardPartitionerRun4 extends BackwardPartitioner{

    public BackwardPartitionerRun4(ADag mDag, PegasusBag mBag)
    {
        super(mDag, mBag);
    }

    public void run4()
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

        while( !mQueue.isEmpty() ){



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
            //System.out.println(se.getSiteHandle() + "'s size limit");
            double limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;
            bigDax.limit = limit;

            //Montage

                if(  (mDag.getChildren(dagJob.jobID).size() > 2) &&
                        (mDag.getParents(dagJob.jobID).size() >=1))
                {

                    //if all its children have been examined then it can processed ,
                    //otherwise no
                    if(mQueue.size() > 1)
                    {
                        mQueue.remove(dagJob);
                        mJobColor.put(dagJob.logicalId, "WHITE");
                        continue;
                    }

                }

            //Cybershake
            // it should be depth realted indeed

            if(i <= 1)
            {



                if ( (getSizeMap(bigDax, dagJob) + getSizeMap(bigDax))  > limit )
                {
                    //build a new dax
                    //printout(dax);
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


                ArrayList jobList = new ArrayList();
                //jobList = getParents(jobList, dagJob);
                jobList = getParents(jobList, dagJob.jobID);
                if( (getSizeMap(bigDax, jobList) + getSizeMap(bigDax)) > limit )
                {
                    //printout(dax);
                    //System.out.println("The size of dax is " + getSizeMap(bigDax) + " + " + getSizeMap(bigDax,jobList));
                    dax = new ADAG("subworkflow" + mDaxMap.size());
                    bigDax = new DAXAbstraction(dax);
                    fList = new HashMap<String, Double>();
                    mDaxMap.put(bigDax, fList);
                    siteIndex ++;
                }

                updateSizeMap(bigDax, dagJob);
                addDAGJob(dagJob, dax);
                mJob2DAX.put(dagJob.getLogicalID(),dax);
                //xxxxxx wrong here all parent
                for(Iterator it = jobList.iterator();it.hasNext();)
                {



                    String jobname = (String)it.next();
                    edu.isi.pegasus.planner.classes.Job parent = getDagJob(jobname);

                    if(jobname.equals(dagJob.jobID))
                    {
                        continue;
                    }

//                    if(!depthMap.containsKey(parent.jobID))
//                        depthMap.put(parent.jobID, i+1);

                    String color = "";
                    if(!mJobColor.containsKey(parent.logicalId))
                    {
                        mJobColor.put(parent.logicalId, "WHITE");
                        color = "WHITE";
                    }
                    else
                    {

                         color = (String)mJobColor.get(parent.logicalId);
                    }
                    //if(!color.equals("GRAY") && !color.equals("BLACK"))
                    if( !color.equals("BLACK"))
                    {

                        updateSizeMap(bigDax, parent);
                        addDAGJob(parent, dax);
                        mJob2DAX.put(parent.getLogicalID(),dax);
                        mJobColor.remove(parent.logicalId);
                        mJobColor.put(parent.logicalId, "BLACK");
                        String cr = (String)mJobColor.get(parent.logicalId);

                    }
                }
                mJobColor.put(dagJob.logicalId, "BLACK");
                mQueue.remove(dagJob);
                //System.out.println("Job " + dagJob.logicalId +" is done");
            }


        }
        super.run();


    }



}
