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
public class BackwardPartitionerRun2 extends BackwardPartitioner{
    public BackwardPartitionerRun2(ADag mDag, PegasusBag mBag)
    {
        super(mDag, mBag);
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

        for( Iterator it = mDag.vJobSubInfos.iterator(); it.hasNext(); ){

            edu.isi.pegasus.planner.classes.Job dagJob =
                    (edu.isi.pegasus.planner.classes.Job)it.next();
            SiteCatalogEntry se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
            double limit = Double.parseDouble(se.getEnvironmentVariable("SITESIZE")) *1e8;


//            double limit = (Double)mSiteSizeMap.get(siteIndex) * 1e8;



            if ( (getSizeMap(bigDax, dagJob) + getSizeMap(bigDax))  > limit)
            {
                //build a new dax
                dax = new ADAG("subworkflow" + mDaxMap.size());
                bigDax = new DAXAbstraction(dax);
                fList = new HashMap<String, Double>();

                mDaxMap.put(bigDax, fList);
                siteIndex ++;

            }
            updateSizeMap(bigDax, dagJob);
            addDAGJob(dagJob, dax);
            mJob2DAX.put(dagJob.getLogicalID(),dax);


        }

        super.run();

    }

}
