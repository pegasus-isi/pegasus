package examples.iterative;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dax.Filename;
import org.griphyn.vdl.dax.Job;
import org.griphyn.vdl.dax.PseudoText;

import java.io.FileWriter;
import java.io.File;
import org.griphyn.vdl.dax.Profile;


/**
 * Creates a leaf dax consisting of a single noop job.
 * 
 * @author vahi
 */
public class CreateLeafDAX{


    public static String NAMESPACE ="pegasus";
    public static String VERSION = "2.0";
    public static String NOOP = "noop";

    public CreateLeafDAX(){
    }

    public void constructDAX(String daxfile){

	try{
	    //construct a dax object
	    ADAG dax = new ADAG(1, 0, "diamond");

	    String id1="ID0000001";

	    //create a job
	    Job job=new Job (NAMESPACE,NOOP,VERSION,id1);
            
            //we add extra condor profiles to make sure it
            //runs as noop job
            job.addProfile( new Profile("pegasus", "noop_job",  new PseudoText( "true" )) );
            job.addProfile( new Profile("pegasus", "noop_job_exit_code",  new PseudoText( "0" )) );
            
            //we add extra hint profiles to make sure 
            //we dont need entry in the tranformation catalog
            job.addProfile( new Profile("hints", "executionPool",  new PseudoText( "local" )) );
            job.addProfile( new Profile("hints", "pfnHint",  new PseudoText( "/bin/true" )) );
            
            
	    //add the job to the dax
	    dax.addJob(job);

             //write DAX to file
	    FileWriter daxFw = new FileWriter( new File( daxfile) );
	    dax.toXML(daxFw, "", null);
	    daxFw.close();
	    
            
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * Usage : CreateDAX daxfile
     *
     * @param args the arguments passed
     */
    public static void main(String[] args) {
        CreateLeafDAX daxgen = new CreateLeafDAX();
        if (args.length == 1) {
            daxgen.constructDAX(args[0]);

        } else {
            System.out.println("Usage: CreateLeafDAX <outputdaxfile>");
        }
    }

}
