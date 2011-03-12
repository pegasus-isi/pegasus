/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.Map;
/**
 *
 * @author wchen
 */
public class CreateSubDAX {

    private SubworkflowPartitioner partitioner;
    public Map getDAXRC()
    {
        return partitioner.getDAXRC();
    }
    public CreateSubDAX(ADag dag, PegasusBag bag){

        SubworkflowPartitioner p = new SubworkflowPartitioner(dag, bag);
        partitioner = p;

    }
    
   
    

    public void addDAXJob()
    {
        partitioner.addDAXJob();
     
    }
   

}
