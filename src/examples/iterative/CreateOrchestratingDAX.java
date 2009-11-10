package examples.iterative;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import examples.iterative.CreateDAX;
import java.io.File;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dax.Job;
import org.griphyn.vdl.dax.DAXJob;
import org.griphyn.vdl.dax.PseudoText;
import org.griphyn.vdl.dax.Profile;

import java.io.FileWriter;

public class CreateOrchestratingDAX{


    public static String NAMESPACE ="pegasus";
    public static String VERSION = "2.0";
    public static String CREATE_ITERATIVE_DAX = "create-iterative-dax";

    /**
     *
     * @param directory
     * @param lfn
     * @param index
     * @return
     */
    public static String getOrchestratingDAXPFN( String directory, String lfn ){
       StringBuffer pfn = new StringBuffer();
       pfn.append( directory ).append( File.separator ).
           append( lfn );

       return pfn.toString();
    }

    public static String getDAXLFN( String basename, int currentIteration ){
        StringBuffer lfn = new StringBuffer();
        lfn.append( basename ).append( "-" ).append( currentIteration ).append( ".dax" );
        return lfn.toString();
    }

    private String mDirectory ;
    private int mMaxIterations;

    public CreateOrchestratingDAX( String directory, int maxIterations ){
        mDirectory = directory;
        

        mMaxIterations = maxIterations;
    }

    public String constructDAX( String daxBasename , String computeDAX, int iteration ){
        String daxFile = null;
	try{
	    //construct a dax object
	    ADAG dax = new ADAG( 100, iteration, "iterative");
            int nextIteration = iteration + 1;

            //create a DAX job that refers to the the compute DAX
	    String id1="ID0000001";
            String daxLFN = new File( computeDAX ).getName();
            DAXJob job1 = new DAXJob( id1, daxLFN );
            job1.setDAXPFN( computeDAX, "local" );
            //add some arguments
            job1.addArgument( new PseudoText( "--dir " + "iteration_" + iteration  + " -vvvvv ") );
	    //add the job to the dax
	    dax.addJob( job1 );

	    //create a job that will generate the dax for the
            //next iteration
            String id2="ID0000002";
	    Job job2 = new Job ( NAMESPACE, CREATE_ITERATIVE_DAX,VERSION,id2);

	    //add the arguments to the job
            job2.addArgument( new PseudoText( mDirectory ) );
            job2.addArgument( new PseudoText( " " ) );
	    job2.addArgument( new PseudoText( Integer.toString( nextIteration ) ) );
            job2.addArgument( new PseudoText( " " ) );
            job2.addArgument( new PseudoText( Integer.toString( mMaxIterations ) ) );

            //we want the job to execute on local site always
            job2.addProfile( new Profile("hints", "executionPool",  new PseudoText( "local" )) );

	    //add the job to the dax
	    dax.addJob( job2 );

	    //create a DAX job that spawns the next iteration.
            //it refers to the orchestrating dax for the next
            //iteration that is to be created by job 2
	    String id3="ID0000003";
            String nextDAXLFN = getDAXLFN( daxBasename, nextIteration );
	    DAXJob job3 = new DAXJob( id3, nextDAXLFN );
            job3.setDAXPFN( getOrchestratingDAXPFN( mDirectory, nextDAXLFN), "local" );
            //add some arguments
            job3.addArgument( new PseudoText( "--dir " + "iteration-" + nextIteration  + " -vvvvv ") );
	    //add the job to the dax
	    dax.addJob( job3 );

	    //add the relationships between the jobs (creating a diamond dependency)

	    dax.addChild(id2,id1);
	    dax.addChild(id3,id2);

	    //write DAX to file
            daxFile =  getOrchestratingDAXPFN( mDirectory,
                                                      getDAXLFN( daxBasename, iteration ));
	    FileWriter daxFw = new FileWriter( daxFile );
	    dax.toXML(daxFw, "", null);
	    daxFw.close();

            return daxFile;
	} catch (Exception e) {
	    e.printStackTrace();
	}
        return daxFile;
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
