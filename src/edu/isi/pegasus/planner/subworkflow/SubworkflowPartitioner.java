/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow;


import edu.isi.pegasus.planner.subworkflow.backward.SiteAbstraction;
import edu.isi.pegasus.planner.subworkflow.backward.BackwardPartitionerImplementation;
import edu.isi.pegasus.planner.subworkflow.backward.DAXAbstraction;
import edu.isi.ikcap.workflows.util.DynamicLoader;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PegasusFile.LINKAGE;
import edu.isi.pegasus.planner.namespace.Hints;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.dax.ADAG;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
/**
 *
 * @author wchen
 */


public class SubworkflowPartitioner {

    private ADag mDag ;
    private Map mJob2DAX;
    private ArrayList mParentDAX;
    private ArrayList mChildDAX;
    private Map mDaxRC;

    private Map mDaxMap;
    private PegasusBag mBag;

    private ArrayList mSiteIndex;
    private String reorder;

    //remove at first and then add
    private List<PCRelation> mReducedMapDag;
    //add at first and then remove
    private List<PCRelation> mReducedMapDag2;
    private LinkedList mDaxQueue;
    public static final String DEFAULT_PACKAGE_NAME =
        "edu.isi.pegasus.planner.subworkflow.backward.implementation";
    public Map getDAXRC()
    {
        return mDaxRC;
    }
    public SubworkflowPartitioner(ADag dag, PegasusBag bag){
        mDag = dag;
        mBag = bag;
        mJob2DAX = new HashMap<String, ADAG>();
        mParentDAX = new ArrayList<String>();
        mChildDAX = new ArrayList<String>();
        mDaxRC =  new HashMap<String, ReplicaLocation>();
        //mDaxSize = new HashMap<String, Double>();
        //mDaxList = new ArrayList<ADAG>();
        mDaxMap = new HashMap<DAXAbstraction, Map>();
        //mSiteMap =  new HashMap<Integer, String>();
        mReducedMapDag = new ArrayList<PCRelation>();
        mReducedMapDag2 = new ArrayList<PCRelation>();
        mSiteIndex = new ArrayList<SiteAbstraction>();
        //mSiteSizeMap = new HashMap<Integer, Double>();
        mDaxQueue = new LinkedList<DAXAbstraction>();

    }


    /**
     * convert the relation in mParentDAX and mChildDAX into mDag.relation
     */
    private void addRelation()
    {

        for(int i = 0; i < mParentDAX.size(); i++)
        {
            String parent = (String)mParentDAX.get(i);
            String child = (String)mChildDAX.get(i);
            mDag.addNewRelation(parent, child);
        }

    }
    private void addReplicaCatalog(String lfn, String pfn)
    {

        ReplicaCatalogEntry rce = new ReplicaCatalogEntry( "file://" + pfn, "local" );
        ArrayList al = new ArrayList();
        al.add(rce);
        ReplicaLocation rc = new ReplicaLocation(lfn, al);
        mDaxRC.put(lfn, rc);

    }

    private void addDAXJob2DAG(String file, String logicalID, String site)
    {

        edu.isi.pegasus.planner.classes.Job j = new edu.isi.pegasus.planner.classes.Job( );
        j.setUniverse( GridGateway.JOB_TYPE.compute.toString() );
        j.setJobType( edu.isi.pegasus.planner.classes.Job.COMPUTE_JOB );
        j.setLogicalID( logicalID );
        DAXJob daxJob = new DAXJob( j );
        PegasusFile pf = new PegasusFile( file );


        pf.setLinkage( LINKAGE.INPUT );
        //the job should be tagged type pegasus
        daxJob.setTypeRecursive();
        //the job should always execute on local site
        //for time being
        daxJob.hints.construct( Hints.EXECUTION_POOL_KEY, "local" );

        //also set a fake executable to be used
        daxJob.hints.construct( Hints.PFN_HINT_KEY, "/tmp/pegasus-plan" );

        //retrieve the extra attribute about the DAX
        daxJob.setDAXLFN( file );
        daxJob.addInputFile( pf );

        //add default name and namespace information
        daxJob.setTransformation( "pegasus",
                                  "pegasus-plan",
                                  Version.instance().toString() );


        daxJob.setDerivation( "pegasus",
                              "pegasus-plan",
                               Version.instance().toString() );

        daxJob.level       = -1;

        daxJob.setName( logicalID);
        String arg_schema = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.argument.schema");
        String arg_dir  = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.argument.dir");
//        String arg_dir = this.mBag.getPlannerOptions().getSubmitDirectory();
//        PlannerOptions p = this.mBag.getPlannerOptions();
//        String pp = p.getBaseSubmitDirectory();
//        String pc = p.getRandomDir();
//        String pd = p.getRandomDirName();
//        String pe = p.getRelativeDirectory();
//        String pa = p.getRelativeSubmitDirectory();
//        String ph = p.getJobnamePrefix();
//        String pb = p.getBasenamePrefix();
        String arg_prop = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.argument.prop");

        String arguments = " -Dpegasus.schema.dax=" + arg_schema
                + " -Dpegasus.user.properties=" + arg_prop
                + " --dir " + arg_dir
                + " --cluster horizontal"
                + " -s " + site
                + " -basename tile-00001"
                + " --force "
                + " --nocleanup";

        daxJob.setSiteHandle(site);
        //daxJob.setExecutionPool();
        daxJob.setArguments(arguments);
        mDag.add(daxJob);

    }


        private void reduceByLabel()
    {
        Vector relation = mDag.dagInfo.relations;
        int sum = 0;
        int l2size = relation.size();
        PCRelation pcr = new PCRelation();
        PCRelation bcr = null;
        PCRelation tcr = null;

        for(int i = 0; i < l2size; i ++)
         {

            PCRelation rel = (PCRelation)relation.get(i - sum);


            if( (rel.parent.contains("mProject") && rel.child.contains("mBackground"))
                    || (rel.parent.contains("mBackground") && rel.child.contains("mAdd"))
                    || (rel.parent.contains("mShrink") && rel.child.contains("mAdd")))
                    {
                        mReducedMapDag.add(rel);
                        relation.remove(rel);
                        sum ++;
                    }
            //For CyberShake
            if(rel.child.contains("_ZipSeis") )
            {
                pcr.setChild(rel.child);
                mReducedMapDag.add(rel);
                relation.remove(rel);
                sum ++;
            }
            if(rel.parent.contains("ZipPSA") || rel.parent.contains("ZipPeakSA"))
            {
                pcr.setParent(rel.parent);
                bcr = rel;
                relation.remove(bcr);
                sum ++;
            }
            if(rel.parent.contains("_ZipSeis"))
            {
                tcr = rel;
                relation.remove(tcr);
                sum ++;
            }

        }

        relation.add(pcr);
        //relation.add(bcr);
        relation.add(tcr);



        mReducedMapDag2.add(pcr);


    }
    private void restore()
    {
        for(Iterator it = mReducedMapDag.iterator(); it.hasNext();)
        {
            PCRelation rel =(PCRelation)it.next();
            mDag.dagInfo.relations.add(rel);
        }
        for(Iterator it = mReducedMapDag2.iterator(); it.hasNext();)
        {
            PCRelation rel =(PCRelation)it.next();
            mDag.dagInfo.relations.remove(rel);
        }


    }
    public void addDAXJob()
    {
        String reduceMethod = mBag.getPegasusProperties().getProperty("pegasus.partition.reduce");

        if(reduceMethod.equals("label"))
            reduceByLabel();

        // The usual way of a dax name is subworkflow0, subworkflow1
        getProperties();
        BackwardPartitionerImplementation p ;

        String className = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.run");
        //String className = "BackwardPartitionerRun2";
        className = (className.indexOf('.') == -1) ? DEFAULT_PACKAGE_NAME + "." + className : className;
        DynamicLoader dl = new DynamicLoader(className);
        Object argList[] = new Object[2];
        argList[0] = mDag;
        argList[1] = mBag;
        try
        {
            p = (BackwardPartitionerImplementation) dl.instantiate(argList);
            p.run();
            mDaxMap = p.getDAXMap();
            mJob2DAX = p.getJob2DAX();

            restore();
            mDag = p.getDag();
            for( Iterator it = mDag.vJobSubInfos.iterator(); it.hasNext(); ){
                edu.isi.pegasus.planner.classes.Job dagJob =
                        (edu.isi.pegasus.planner.classes.Job)it.next();
               Vector parentList = mDag.getParents(dagJob.getID());
               for(Iterator pIt = parentList.iterator();pIt.hasNext();)
               {
                   String parent = (String)pIt.next();
                   String parentName = mDag.getSubInfo(parent).getLogicalID();
                   String childName = dagJob.getLogicalID();
                   ADAG childDAX = (ADAG)mJob2DAX.get(childName);
                   ADAG parentDAX = (ADAG)mJob2DAX.get(parentName);
                   if(childDAX.equals(parentDAX))
                   {
                       childDAX.addDependency(parentName, childName);
                   }
                   else
                   {
                    //add dependency between sub workflows
                       //this is a pair


                       if(parentDAX.mName.contains("subworkflow0"))
                       {
                           System.out.println("problem");
                           System.out.println(parent);
                           System.out.println(childName);
                       }
                       mParentDAX.add(parentDAX.mName);
                       mChildDAX.add(childDAX.mName);

                   }

               }

            }

            mDag.clearJobs();

            //Site Selection
            if((reorder==null) || reorder.equals("no"))
                siteSelector0();
            else if(reorder.equals("slot"))
                siteSelector1();



            addRelation();

            printOut();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
    private void printOut()
    {
        System.out.println("\n\n There are " + mDaxMap.size() + " sub workflows\n");


    }
    private void siteSelector0()
    {
                int siteIndex = 0;
        for (Iterator it  = mDaxMap.keySet().iterator(); it.hasNext();)
        {
            ADAG daxItem =((DAXAbstraction)it.next()).adag;
            //ADAG daxItem = (ADAG)it.next();

            String daxName = daxItem.mName + ".xml";
            String dax_path = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.argument.dir")
                    + "/" + daxName;

            daxItem.writeToFile(dax_path);
            addReplicaCatalog(daxName , dax_path);
            try
            {

                  //SiteCatalogEntry se = (SiteCatalogEntry)mSiteIndex.get(siteIndex);
                  SiteAbstraction site = (SiteAbstraction)mSiteIndex.get(siteIndex);
                  String siteName = site.name;
                  addDAXJob2DAG(daxName, daxItem.mName , siteName);



            }
            catch (Exception e)
            {

                System.out.println("no such site name\n");
            }

            siteIndex ++;

        }
    }
    private void siteSelector1()
    {
        int siteIndex = 0;
        //calculate the dax
        String subPath = this.mBag.getPegasusProperties().getProperty("pegasus.subworkflow.argument.dir");
        //first select mDaxMap in order of dax size and then slot
        mDaxQueue.clear();
        for(Iterator it = mDaxMap.keySet().iterator(); it.hasNext();)
        {
            DAXAbstraction bigDax =((DAXAbstraction)it.next());
            //ADAG daxItem = bigDax.adag;
            double limit = bigDax.limit;
            double degree = bigDax.degree;
            if(mDaxQueue.size()==0)
            {
                mDaxQueue.add(bigDax);
                continue;
            }

            for (int i = 0; i < mDaxQueue.size() ; i ++)
            {
                //wrong totally wrong
                DAXAbstraction headDax = (DAXAbstraction)mDaxQueue.get(i);
                if(limit > headDax.limit || (limit == headDax.limit && degree > headDax.degree))
                {
                    mDaxQueue.add(i, bigDax);
                    break;
                }
                if(i == (mDaxQueue.size() -1 ))
                {
                    mDaxQueue.addLast(bigDax);
                    break;
                }

            }
        }

        ArrayList threadList  = new ArrayList<writeToFile>();
        //second select site in order of limit and slot
        for (Iterator it  = mDaxQueue.iterator(); it.hasNext();)
        {
            DAXAbstraction bigDax =((DAXAbstraction)it.next());
            ADAG daxItem = bigDax.adag;

            String daxName = daxItem.mName + ".xml";
            String dax_path = subPath + "/" + daxName;

            threadList.add( new writeToFile(daxItem, dax_path));
            //daxItem.writeToFile(dax_path);
            addReplicaCatalog(daxName , dax_path);
            try
            {

                double limit = bigDax.size;
                int maxSlot = 0;
                SiteAbstraction selectedSite = null;
                for(int i = 0; i < mSiteIndex.size(); i++)
                {
                    SiteAbstraction site = (SiteAbstraction)mSiteIndex.get(i);
                    if (site.space < limit) continue;
                    if (site.slot > maxSlot)
                    {
                        maxSlot = site.slot;
                        selectedSite = site;
                    }
                }
                if(selectedSite==null)
                    throw(new Exception("Size Limit Exceed"));
                else if(true)
                {
                    selectedSite.space = selectedSite.space - limit;
                }
                else
                  mSiteIndex.remove(selectedSite);

                  addDAXJob2DAG(daxName, daxItem.mName , selectedSite.name);
                  System.out.println("dax " + daxName + " with size " + bigDax.size + " with degree " + bigDax.degree +
                          " is mapped to " + selectedSite.name + " with slot " + selectedSite.slot);



            }
            catch (Exception e)
            {

                System.out.println("no such site name\n");
            }

            siteIndex ++;

        }
        for (Iterator it = threadList.iterator(); it.hasNext();)
        {
            writeToFile wt = (writeToFile)it.next();
            wt.getDone();
        }
    }
    private void getProperties()
    {
        try
        {

            SiteStore mSite = mBag.getHandleToSiteStore();

            for (Iterator it = mSite.entryIterator();it.hasNext();)
            {
                SiteCatalogEntry siteEntry = (SiteCatalogEntry)it.next();
                String siteName = siteEntry.getSiteHandle();
                if(siteName.equals("local"))continue;
                double size = Double.parseDouble(siteEntry.getEnvironmentVariable("SITESIZE")) * 1e8;

                int slot    = Integer.parseInt(siteEntry.getEnvironmentVariable("SLOT"));

                SiteAbstraction site = new SiteAbstraction(siteName, size, slot);
                mSiteIndex.add(site);
                System.out.println(siteName+" has been selected");

            }
            reorder = mBag.getPegasusProperties().getProperty("pegasus.subworkflow.reorder");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
