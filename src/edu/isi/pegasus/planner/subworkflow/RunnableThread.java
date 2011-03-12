/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.dax.ADAG;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 *
 * @author wchen
 *
 **/
class RunnableThreadA implements Runnable {

    Thread runner;
    public RunnableThreadA(){

    }
    public RunnableThreadA(String name)
    {
        runner = new Thread(this, name);
        System.out.println(runner.getName());
        //runner.start();
    }
    public void run()
    {
        System.out.println("Running" + Thread.currentThread());
    }
}
class increaseLeafThread implements Runnable{
    Thread runner;
    private int start;
    private int end;
    private String name;
    public boolean done;
    private ADag mDag ;
    private Vector leaf;

    edu.isi.pegasus.planner.classes.Job head;
    edu.isi.pegasus.planner.classes.Job rear;
    public void getDone()
    {
        while(!done)
        {
            
        }
    }
    public increaseLeafThread(int start, int end, ADag mDag, String name,
            edu.isi.pegasus.planner.classes.Job head,
            edu.isi.pegasus.planner.classes.Job rear, Vector leaf)
    {
        runner = new Thread(this, name);
        this.start  = start;
        this.end    = end;
        this.name   = name;
        this.mDag   = mDag;
        this.head   = head;
        this.rear   = rear;
        this.done   = false;
        this.leaf   = leaf;
        runner.start();
        

    }
    public void run()
    {

        for(int i = start; i < end; i++)
        {
            String jobname = (String)leaf.get(i);
            edu.isi.pegasus.planner.classes.Job job =mDag.getSubInfo(jobname);
            synchronized(this){
                mDag.addNewRelation(job.logicalId, rear.logicalId);
            }
        }
        this.done   = true;
    }
}


class increaseRootThread implements Runnable{
    Thread runner;
    private int start;
    private int end;
    private String name;
    public boolean done;
    private ADag mDag ;
    private Vector root;
    edu.isi.pegasus.planner.classes.Job head;
    edu.isi.pegasus.planner.classes.Job rear;
    public void getDone()
    {
        while(!done)
        {

        }
    }
    public increaseRootThread(int start, int end, ADag mDag, String name,
            edu.isi.pegasus.planner.classes.Job head,
            edu.isi.pegasus.planner.classes.Job rear, Vector root)
    {
        runner = new Thread(this, name);
        this.start  = start;
        this.end    = end;
        this.name   = name;
        this.mDag   = mDag;
        this.head   = head;
        this.rear   = rear;
        this.done   = false;
        this.root   = root;
        runner.start();


    }
    public void run()
    {
 
        for(int i = start; i < end; i++)
        {
            String jobname = (String) root.get(i);
            edu.isi.pegasus.planner.classes.Job job = mDag.getSubInfo(jobname);
            synchronized(this){
                mDag.addNewRelation(head.logicalId, job.logicalId);
            }

        }
        this.done   = true;
    }
}
 class writeToFile implements Runnable
{
     private ADAG dax;
     Thread runner;
     private boolean done;
     private String daxPath;
     public void getDone()
     {
         while(!done){
             try
             {
                Thread.currentThread().sleep(10000);
             }
             catch (Exception e)
             {
                 e.printStackTrace();
             }

         }
     }
     public writeToFile(ADAG dax, String daxPath)
     {
         runner = new  Thread(this, "");
        this.dax = dax;
        this.daxPath = daxPath;
        this.done   = false;
        runner.start();
     }
    public void run()
    {
        dax.writeToFile(daxPath);
        this.done = true;
    }
}

 class cleanColorThread implements Runnable{
     Thread runner;
     private int start;
     private int end;
     private ADag mDag;
     private Map mJobColor;
     public boolean done;
     edu.isi.pegasus.planner.classes.Job head;
     edu.isi.pegasus.planner.classes.Job rear;
     public cleanColorThread(){};
     public cleanColorThread(int start, int end, ADag mDag, Map mJobColor,
            edu.isi.pegasus.planner.classes.Job head,
            edu.isi.pegasus.planner.classes.Job rear)
     {
                 runner = new Thread(this, "");
        this.start  = start;
        this.end    = end;
        this.mDag   = mDag;
        this.mJobColor = mJobColor;
        this.done   = false;
        this.head   = head;
        this.rear   = rear;
        runner.start();
         
     }
     public void run()
     {

        for(int i = start; i < end; i++)
        {
            edu.isi.pegasus.planner.classes.Job job =
            (edu.isi.pegasus.planner.classes.Job) mDag.vJobSubInfos.get(i);
            mJobColor.put(job.logicalId, "WHILE");
            int size = mDag.getParents(job.jobID).size();
            if(size==0)
            {
                mDag.addNewRelation(head.logicalId, job.logicalId);
            }

            size = mDag.getChildren(job.jobID).size();
            if(size==0)
            {
                mDag.addNewRelation(job.logicalId, rear.logicalId);
            }

        }
        this.done   = true;
         
     }
}



public class RunnableThread{
    public static void main(String[] args)
    {
        Map map = new HashMap<String, Double>();
        map.put("A", 1.1);
        map.put("B", 2.2);
        //getSizeMapThread thread1 = new getSizeMapThread(0,2,map,"");
        //thread1.runner.start();
        //double sum  = thread1.getSum();
        try{
            Thread.currentThread().sleep(1000);
        }catch (Exception e)
        {

        }
        //System.out.println(sum);
    }

}