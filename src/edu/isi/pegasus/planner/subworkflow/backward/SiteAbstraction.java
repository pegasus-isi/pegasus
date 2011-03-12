/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow.backward;

/**
 *
 * @author wchen
 */
public  class SiteAbstraction{

     public String name;
     public double size;
     public int    slot;
     public double memory;
     public double loadAve;
     public double kFlops;
     public double totalRAM;
     public double mips;
     public double queueWaitTime;
     public double space;
     public SiteAbstraction(String name, double size, int slot)
     {
         this.name  = name;
         this.size  = size;
         this.slot  = slot;
         this.space = size;

     }
}
