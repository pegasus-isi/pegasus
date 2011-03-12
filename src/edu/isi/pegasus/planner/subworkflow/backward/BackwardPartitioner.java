/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow.backward;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.dax.File.LINK;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Vector;
/**
 *
 * @author wchen
 */

public class BackwardPartitioner implements BackwardPartitionerImplementation {

    protected ADag mDag ;
    protected Map mJob2DAX;
    protected Map mDaxMap;
    protected LinkedList mSiteIndex;
    protected LinkedList mQueue;
    protected PegasusBag mBag;
    protected Map mID2LogicalID;
    protected Map mIDorLogicalID2Job;
    protected Map depthMap;
    protected Map mJobColor;
    protected Map mBreathMap;
    protected Map mdax2bigdax;
    protected Map mIndexSiteList;
    public Map getDAXMap()
    {

        String reorder = mBag.getPegasusProperties().getProperty("pegasus.subworkflow.reorder");
        if(reorder==null || reorder.equals("no"))
            return mDaxMap;


        for(Iterator it = mDaxMap.keySet().iterator(); it.hasNext(); )
        {
            DAXAbstraction bigDax = (DAXAbstraction)it.next();
            ADAG dax = bigDax.adag;
            bigDax.size = getSizeMap(bigDax);
            mdax2bigdax.put(dax, bigDax);

        }
        edu.isi.pegasus.planner.classes.Job rear = getRear();
        mBreathMap.put(rear.logicalId, 0);
        ADAG dax = (ADAG)mJob2DAX.get(rear.logicalId);
        DAXAbstraction bigDax = (DAXAbstraction)mdax2bigdax.get(dax);
        ArrayList list = new ArrayList();
        list.add(rear);
        bigDax.depthMap.put(0, list);
        iterate2depth(0, rear.jobID);
        System.out.println(mDag.getNoOfJobs() + " and " + mBreathMap.size());

        for(Iterator it = mDaxMap.keySet().iterator(); it.hasNext(); )
        {
            bigDax = (DAXAbstraction)it.next();
            int maxBreath = 0;
            for( Iterator itt = bigDax.depthMap.values().iterator(); itt.hasNext(); )
            {
                //Object obj = itt.next();
                list = (ArrayList)itt.next();
                if(list.size() > maxBreath)
                    maxBreath = list.size();
            }
            bigDax.degree = maxBreath;

        }

        return mDaxMap;
    }
    protected void iterate2depth(int depth, String jobID)
    {
        depth ++;
        for(Iterator it = mDag.getParents(jobID).iterator();it.hasNext();)
        {
            String jobname = (String) it.next();
            String logicalid = jobID2jobLogicalID(jobname);
            if(!mBreathMap.containsKey(logicalid))
            {
                mBreathMap.put(logicalid, depth );
                iterate2depth(depth , jobname);
                if(!mJob2DAX.containsKey(logicalid))
                {
                    System.out.println("What's wrong?");
                }
                ADAG dax = (ADAG)mJob2DAX.get(logicalid);
                DAXAbstraction bigDax = (DAXAbstraction)mdax2bigdax.get(dax);
                if(!bigDax.depthMap.containsKey(depth))
                {
                    ArrayList list = new ArrayList();
                    bigDax.depthMap.put(depth, list);
                }
                ArrayList list = (ArrayList)bigDax.depthMap.get(depth );
                //?????????
                edu.isi.pegasus.planner.classes.Job dagJob = getDagJob(jobname);
                if(!list.contains(dagJob))
                {
                    list.add(dagJob);
                }
            }



        }
    }

    public Map getJob2DAX(){return mJob2DAX;}


    public ADag getDag()
    {
            return mDag;
    }
    public BackwardPartitioner(ADag mDag, PegasusBag mBag)
    {
        this.mDag = mDag;
        this.mBag = mBag;

        mSiteIndex = new LinkedList<SiteCatalogEntry>();
        mJob2DAX = new HashMap<String, ADAG>();
        mDaxMap = new HashMap<DAXAbstraction, Map>();
        mJobColor = new HashMap<String, String>();
        mQueue = new LinkedList();
        depthMap = new HashMap<String, Integer>();
        mBreathMap = new HashMap<String, Integer>();
        mID2LogicalID = new HashMap<String, String>();
        mIDorLogicalID2Job = new HashMap<String, edu.isi.pegasus.planner.classes.Job>();
        mdax2bigdax = new HashMap<ADAG, DAXAbstraction>();
        mIndexSiteList = new HashMap<Integer, LinkedList>();
    }
    protected double getSizeMap(DAXAbstraction bigDax, edu.isi.pegasus.planner.classes.Job dagJob)
    {

        Map fList = (Map)mDaxMap.get(bigDax);
        double sum = 0.0;
        if(fList.size() <= 0)
            return sum;
        for(Iterator inFilesIt = dagJob.getInputFiles().iterator(); inFilesIt.hasNext();)
            {
               //
                PegasusFile input = (PegasusFile)inFilesIt.next();
                String fileName = input.getLFN();

                if(input.getType()==1)
                    continue;
                double size = input.getSize();
                if(!fList.containsKey(fileName))
                {
                    sum += size;
                }
            }
        for(Iterator outFilesIt = dagJob.getOutputFiles().iterator(); outFilesIt.hasNext();)
        {
            PegasusFile output = (PegasusFile)outFilesIt.next();
            String fileName = output.getLFN();
            if (output.getType()==1)
                continue;
            double size = output.getSize();
            if(!fList.containsKey(fileName))
            {
                sum += size;
            }
        }

        return sum;
    }
    protected double getSizeMap(DAXAbstraction bigDax, ArrayList dagJobList)
    {
        Map fList = (Map)mDaxMap.get(bigDax);
        double sum = 0.0;
        if(fList.size() <= 0)
            return sum;

        Map map = new HashMap();
        for (Iterator jobIt = dagJobList.iterator();jobIt.hasNext();)
        {
            edu.isi.pegasus.planner.classes.Job dagJob =
                    getDagJob((String)jobIt.next());
//                    (edu.isi.pegasus.planner.classes.Job)jobIt.next();
            for(Iterator inFilesIt = dagJob.getInputFiles().iterator(); inFilesIt.hasNext();)
            {
               //
                PegasusFile input = (PegasusFile)inFilesIt.next();
                String fileName = input.getLFN();

                if(input.getType()==1)
                    continue;
                double size = input.getSize();

                if(!map.containsKey(fileName))
                {
                    map.put(fileName, size);
                }

            }
            for(Iterator outFilesIt = dagJob.getOutputFiles().iterator(); outFilesIt.hasNext();)
            {
                PegasusFile output = (PegasusFile)outFilesIt.next();
                String fileName = output.getLFN();
                if (output.getType()==1)
                    continue;
                double size = output.getSize();
                if(!map.containsKey(fileName))
                {
                    map.put(fileName, size);
                }

            }
        }
        for(Iterator it = map.keySet().iterator(); it.hasNext();)
        {
            String fileName = (String)it.next();
            double size     = (Double)map.get(fileName);
            if(!fList.containsKey(fileName))
            {
                sum += size;
            }
        }
        return sum;
    }


        protected double getSizeMap(ArrayList dagJobList, int siteIndex)
    {
        LinkedList siteList = (LinkedList)mIndexSiteList.get(siteIndex);
        double sum = 0.0;
        Map map = new HashMap();
        for (Iterator jobIt = dagJobList.iterator();jobIt.hasNext();)
        {
            edu.isi.pegasus.planner.classes.Job dagJob =
                    getDagJob((String)jobIt.next());
            for(Iterator inFilesIt = dagJob.getInputFiles().iterator(); inFilesIt.hasNext();)
            {
               //
                PegasusFile input = (PegasusFile)inFilesIt.next();
                String fileName = input.getLFN();

                if(input.getType()==1)
                    continue;
                double size = input.getSize();

                if(!map.containsKey(fileName))
                {
                    map.put(fileName, size);
                }

            }
            for(Iterator outFilesIt = dagJob.getOutputFiles().iterator(); outFilesIt.hasNext();)
            {
                PegasusFile output = (PegasusFile)outFilesIt.next();
                String fileName = output.getLFN();
                if (output.getType()==1)
                    continue;
                double size = output.getSize();
                if(!map.containsKey(fileName))
                {
                    map.put(fileName, size);
                }

            }
        }

        for(Iterator it = map.keySet().iterator(); it.hasNext();)
        {
            String fileName = (String)it.next();
            double size     = (Double)map.get(fileName);
            boolean hasFile = false;
            for(Iterator daxIt = siteList.iterator(); daxIt.hasNext();)
            {
                DAXAbstraction bigDax = (DAXAbstraction)daxIt.next();
                Map fList = (Map)mDaxMap.get(bigDax);
                if(fList.size() <=0 )continue;

                if(fList.containsKey(fileName))
                {
                    hasFile = true;
                    break;

                }
            }
            if (!hasFile)
            {
                sum += size;
            }
        }
        return sum;
    }


    /**
     * Calculate the file size of all dax
     * @param dax
     * @return
     */
    protected double getSizeMap(DAXAbstraction bigDax)
    {
        double sum = 0.0;
        Map fList = (Map)mDaxMap.get(bigDax);

        int fListsize = fList.size();
        if (fListsize <= 0 )return sum;

              for(Iterator it = fList.values().iterator(); it.hasNext();)
                {
                    double size = (Double)it.next();
                    sum += size;
                }



        return sum;
    }

    protected ArrayList getParents(ArrayList jobList, String jobID)
    {


        int  depth = (Integer)depthMap.get(jobID);
        for(Iterator it = mDag.getParents(jobID).iterator();it.hasNext();)
        {
            String jobname = (String )it.next();
            //edu.isi.pegasus.planner.classes.Job parent = getDagJob(jobname);
            String logicalID = jobID2jobLogicalID(jobname);
            String color = (String)mJobColor.get(logicalID);

            if(!depthMap.containsKey(jobname))
                depthMap.put(jobname, depth+1);
            if((color==null)|| (!color.equals("BLACK")&& !color.equals("GRAY")))

            {
                  if(!jobList.contains(jobname))
                    {
                        jobList.add(jobname);
                        jobList = getParents(jobList, jobname);

                    }
            }



        }
        for(Iterator it = mDag.getChildren(jobID).iterator();it.hasNext();)
        {
            String jobname = (String )it.next();
            //edu.isi.pegasus.planner.classes.Job child = getDagJob(jobname);
            String logicalID = jobID2jobLogicalID(jobname);
            String color = (String)mJobColor.get(logicalID);
            if(!depthMap.containsKey(jobname))
                depthMap.put(jobname, depth-1);
            int i = (Integer)depthMap.get(jobname);
            if((color==null)||( !color.equals("BLACK")&& ! (color.equals("GRAY"))))
            {
                if(!jobList.contains(jobname))
                {
                    jobList.add(jobname);
                    jobList = getParents(jobList, jobname);

                }
            }
            else if((color!=null)&& (color.equals("GRAY") && i >=2))
            {

                if(!jobList.contains(jobname))
                {
                    jobList.add(jobname);

                }
            }
            else if(jobname.contains("ZipSeis"))
            {

                if(!jobList.contains(jobname))
                {
                    jobList.add(jobname);

                }
            }



        }

        return jobList;
    }


      protected edu.isi.pegasus.planner.classes.Job getRear()
      {
          edu.isi.pegasus.planner.classes.Job rear =
                  (edu.isi.pegasus.planner.classes.Job)mDag.vJobSubInfos.get(0);

          mQueue.clear();
          mQueue.addLast(rear);
          while(!mQueue.isEmpty())
          {
              edu.isi.pegasus.planner.classes.Job dagJob =
                    (edu.isi.pegasus.planner.classes.Job)mQueue.getLast();
              Vector vec = mDag.getChildren(dagJob.jobID);
              if(vec.size() == 0)
              {
                    rear = dagJob;
                    break;
              }
              else
              {
                  mQueue.remove(dagJob);
                  mQueue.add(vec.get(0));
              }
          }
          return rear;
      }
    //Here we only add but not delete jobs, so there is less concern about synchronization

    public void run()
    {

      mQueue.clear();
      mJobColor.clear();
    }

    protected boolean checkChildColor(String jobID)
    {
        boolean val = true;
          for(Iterator it = mDag.getChildren(jobID).iterator();it.hasNext();)
           {

                    String jobname = (String )it.next();
                    edu.isi.pegasus.planner.classes.Job children = getDagJob(jobname);
                    String color = (String)mJobColor.get(children.logicalId);
                    if(color==null || !color.equals("BLACK"))
                    {
                        val = false; break;
                    }
          }

        return val;
    }
      

    protected void translate()
    {
        for(Iterator it = mDag.jobIterator(); it.hasNext(); )
        {
            edu.isi.pegasus.planner.classes.Job job =
                    (edu.isi.pegasus.planner.classes.Job)it.next();

            mID2LogicalID.put(job.jobID, job.logicalId);
            mIDorLogicalID2Job.put(job.logicalId, job);
            mIDorLogicalID2Job.put(job.jobID, job);

        }
    }
    protected String jobID2jobLogicalID(String jobID)
    {
        return (String)mID2LogicalID.get(jobID);
    }

    protected edu.isi.pegasus.planner.classes.Job getDagJob(String jobname)
    {
//        for(Iterator it = mDag.jobIterator(); it.hasNext(); )
//        {
//            edu.isi.pegasus.planner.classes.Job job =
//                    (edu.isi.pegasus.planner.classes.Job)it.next();
//            if(jobname.equals(job.jobID) || jobname.equals(job.logicalId))
//                return job;
//        }
//        return null;
        return (edu.isi.pegasus.planner.classes.Job)mIDorLogicalID2Job.get(jobname);
    }

    protected void addDAGJob(edu.isi.pegasus.planner.classes.Job dagJob, ADAG dax)
    {

            String args = dagJob.getArguments();
            String id = dagJob.getLogicalID();
            String name = dagJob.getTXName();
            String namespace = dagJob.getTXNamespace();
            String version = dagJob.getTXVersion();
            edu.isi.pegasus.planner.dax.Job daxJob =new edu.isi.pegasus.planner.dax.Job(id,
                namespace, name, version);
            daxJob.addArgument(args);
            for(Iterator inFilesIt = dagJob.getInputFiles().iterator(); inFilesIt.hasNext();)
            {
               //
                PegasusFile input = (PegasusFile)inFilesIt.next();
                String fileName = input.getLFN();
                edu.isi.pegasus.planner.dax.File fi =
                        new edu.isi.pegasus.planner.dax.File(fileName);
                if(input.getType()==1)
                    fi.setExecutable(true);
                double size = input.getSize();

                daxJob.uses(fi, LINK.input);

            }
            for(Iterator outFilesIt = dagJob.getOutputFiles().iterator();outFilesIt.hasNext();)
            {
                PegasusFile output = (PegasusFile)outFilesIt.next();
                String fileName = output.getLFN();
                edu.isi.pegasus.planner.dax.File fo =
                        new edu.isi.pegasus.planner.dax.File(fileName);
                daxJob.uses(fo, LINK.output);
            }
            dax.addJob(daxJob);
    }

    protected void updateSizeMap(DAXAbstraction bigDax, edu.isi.pegasus.planner.classes.Job dagJob)
    {

        Map fList = (Map)mDaxMap.get(bigDax);
       for(Iterator inFilesIt = dagJob.getInputFiles().iterator(); inFilesIt.hasNext();)
            {
               //
                PegasusFile input = (PegasusFile)inFilesIt.next();
                String fileName = input.getLFN();

                if(input.getType()==1)
                    return;
                double size = input.getSize();
//                updateSizeMap(fileName, size);


                if (!fList.containsKey(fileName))
                {
                    fList.put(fileName, size);
                }
            }
        for(Iterator outFilesIt = dagJob.getOutputFiles().iterator(); outFilesIt.hasNext();)
        {
            PegasusFile output = (PegasusFile)outFilesIt.next();
            String fileName = output.getLFN();
            if (output.getType()==1)
                continue;
            double size = output.getSize();
            if(!fList.containsKey(fileName))
            {
                fList.put(fileName, size);
            }
        }
    }

    protected void getProperties()

    {
        try
        {
            mSiteIndex.clear();
            SiteStore mSite = mBag.getHandleToSiteStore();
            String reorder = mBag.getPegasusProperties().getProperty("pegasus.subworkflow.reorder");
            if( (reorder==null) || reorder.equals("no"))
            {
                for (Iterator it = mSite.entryIterator();it.hasNext();)
                {
                    SiteCatalogEntry siteEntry = (SiteCatalogEntry)it.next();
                    String site = siteEntry.getSiteHandle();
                    if(site.equals("local"))continue;
                    mSiteIndex.add(siteEntry);
                    System.out.println(site+" has been selected in BackwardPartitioner.java");
                }
            }
            else
            {

                for (Iterator it = mSite.entryIterator();it.hasNext();)
                {
                    SiteCatalogEntry siteEntry = (SiteCatalogEntry)it.next();
                    String site = siteEntry.getSiteHandle();
                    if(site.equals("local"))continue;
                    double limit = Double.parseDouble(siteEntry.getEnvironmentVariable("SITESIZE")) *1e8;
                    if(mSiteIndex.size() == 0)
                    {
                        mSiteIndex.add(siteEntry);
                    }
                    else
                    {
                        for(int i = 0; i < mSiteIndex.size(); i++)
                        {
                            SiteCatalogEntry curSiteEntry = (SiteCatalogEntry)mSiteIndex.get(i);
                            double curLimit = Double.parseDouble(curSiteEntry.getEnvironmentVariable("SITESIZE")) *1e8;
                            if(curLimit < limit)
                            {
                                mSiteIndex.add(i, siteEntry);
                                break;
                            }
                            if( i == (mSiteIndex.size() -1) )
                            {
                                mSiteIndex.addLast(siteEntry);
                                break;
                            }

                        }
                    }

                }
                for(int i = 0 ; i < mSiteIndex.size(); i ++)
                {
                   SiteCatalogEntry curSiteEntry = (SiteCatalogEntry)mSiteIndex.get(i);
                   double curLimit = Double.parseDouble(curSiteEntry.getEnvironmentVariable("SITESIZE"));
                   System.out.println("site: " + curSiteEntry.getSiteHandle() +" " + curLimit);
                }
                //System.out.println();

            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }





}
