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

/**
 * Creates a diamond dax.
 *
 * @author vahi
 */
public class CreateDAX{


    public static String NAMESPACE ="diamond";
    public static String VERSION = "2.0";
    public static String PREPROCESS = "preprocess";
    public static String FINDRANGE = "findrange";
    public static String ANALYZE = "analyze";
    public static String FA="f.a";
    public static String FB1="f.b1";
    public static String FB2="f.b2";
    public static String FC1="f.c1";
    public static String FC2="f.c2";
    public static String FD="f.d";

    /**
     * The default constructor.
     */
    public CreateDAX(){
    }

    /**
     * Construct the dax.
     *
     * @param daxfile  the dax file to be constructed.
     */
    public void constructDAX(String daxfile){

	try{
	    //construct a dax object
	    ADAG dax = new ADAG(1, 0, "diamond");

	    String id1="ID0000001";

	    //create a job
	    Job job=new Job (NAMESPACE,PREPROCESS,VERSION,id1);
	    //add the arguments to the job
	    job.addArgument(new PseudoText("-a preprocess "));
	    job.addArgument(new PseudoText("-T60 "));
	    job.addArgument(new PseudoText("-i "));
	    job.addArgument(new Filename(FA));
	    job.addArgument(new PseudoText(" -o "));
	    job.addArgument(new Filename(FB1));
	    job.addArgument(new PseudoText(" "));
	    job.addArgument(new Filename(FB2));
	    //add the files used by the job

	    job.addUses(new Filename(FA,LFN.INPUT));
	    Filename f=new Filename(FB1,LFN.OUTPUT);
	    f.setRegister( false );
	    job.addUses(f);
	    f=new Filename(FB2,LFN.OUTPUT);
	    f.setRegister( false );
	    job.addUses(f);

	    //add the job to the dax
	    dax.addJob(job);


	    //create second  job

	    String id2="ID0000002";
	    job=new Job (NAMESPACE,FINDRANGE,VERSION,id2);
	    //add the arguments to the job
	    job.addArgument(new PseudoText("-a findrange "));
	    job.addArgument(new PseudoText("-T60 "));
	    job.addArgument(new PseudoText("-i "));
	    job.addArgument(new Filename(FB1));
	    job.addArgument(new PseudoText(" -o "));
	    job.addArgument(new Filename(FC1));
	    //add the files used by the job

	    job.addUses(new Filename(FB1,LFN.INPUT));
	    job.addUses(new Filename(FC1,LFN.OUTPUT));

	    //add the job to the dax
	    dax.addJob(job);

	    //create third job

	    String id3="ID0000003";
	    job=new Job (NAMESPACE,FINDRANGE,VERSION,id3);
	    //add the arguments to the job
	    job.addArgument(new PseudoText("-a findrange "));
	    job.addArgument(new PseudoText("-T60 "));
	    job.addArgument(new PseudoText("-i "));
	    job.addArgument(new Filename(FB2));
	    job.addArgument(new PseudoText(" -o "));
	    job.addArgument(new Filename(FC2));
	    //add the files used by the job

	    job.addUses(new Filename(FB2,LFN.INPUT));
	    job.addUses(new Filename(FC2,LFN.OUTPUT));

	    //add the job to the dax
	    dax.addJob(job);


	    //create fourth job //analyze
	    //To be uncommented for exercise 2.1

	    String id4="ID0000004";
	    job=new Job (NAMESPACE,ANALYZE,VERSION,id4);

	    //add the arguments to the job
	    job.addArgument(new PseudoText("-a analyze "));
	    job.addArgument(new PseudoText("-T60 "));
	    job.addArgument(new PseudoText("-i "));
	    job.addArgument(new Filename(FC1));
	    job.addArgument(new PseudoText(" "));
	    job.addArgument(new Filename(FC2));
	    job.addArgument(new PseudoText(" -o "));
	    job.addArgument(new Filename(FD));

	    //add the files used by the job
	    job.addUses(new Filename(FC1,LFN.INPUT));
	    job.addUses(new Filename(FC2,LFN.INPUT));
	    job.addUses(new Filename(FD,LFN.OUTPUT));

	    //add the job to the dax
	    dax.addJob(job);

	    //the job with id4 is a child to both jobs id2 and id3
	    dax.addChild(id4,id2);
	    dax.addChild(id4,id3);

	    //End of commented out code for Exercise 2.1

	    //add the relationships between the jobs (creating a diamond dependency)

	    dax.addChild(id2,id1);
	    dax.addChild(id3,id1);


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
        CreateDAX daxgen = new CreateDAX();
        if (args.length == 1) {
            daxgen.constructDAX(args[0]);

        } else {
            System.out.println("Usage: CreateDAX <outputdaxfile>");
        }
    }

}
