package examples.iterative;

/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
import examples.iterative.CreateDAX;
import java.io.File;



/**
 * An example class that creates iterative DAX'es using the recursive
 * DAX feature in Pegasus.
 *
 * The class generates the following DAX files
 *      - a compute DAX
 *      - 3.0 orchestrating DAX
 *
 * The 3.0 orchestrating DAX contains 3 jobs
 *
 *      - a dax job with reference to the compute dax
 *      - a compute job that invokes create-iterative-dax for the next iteration
 *      - a dax job referencing to 3.0 DAX for the next iteration
 *
 * @author Karan Vahi
 */
public class CreateIterativeDAX {

    public static final String ORCHESTRATING_DAX_BASENAME = "iterative";

    public static final String COMPUTE_DAX_BASENAME = "diamond";

    private int mMaxIterations;


    /**
     * The directory where the dax files are created.
     */
    private String mDirectory;

    public CreateIterativeDAX( int maxIterations ){
        mMaxIterations = maxIterations;
    }

    public static String getDAXLFN( String basename, int currentIteration ){
        StringBuffer lfn = new StringBuffer();
        lfn.append( basename ).append( "-" ).append( currentIteration ).append( ".dax" );
        return lfn.toString();
    }

    public String constructIterativeDAX( String directory, int currentIteration ){
        mDirectory = directory;
        //create the directory if does not exist
        File dir = new File( mDirectory );
        dir.mkdirs();
        //resolve to full path
        mDirectory = dir.getAbsolutePath();

        //sanity check
        if( currentIteration == mMaxIterations ){
            //create the LEAF DAX and exit
        }

        String computeDAXFile = new File( mDirectory, getDAXLFN( COMPUTE_DAX_BASENAME, currentIteration )).getAbsolutePath();
        CreateDAX createComputeDAX = new CreateDAX();
        createComputeDAX.constructDAX( computeDAXFile );

        CreateOrchestratingDAX createOrchestratingDAX = new CreateOrchestratingDAX( mDirectory , mMaxIterations );
        return createOrchestratingDAX.constructDAX( ORCHESTRATING_DAX_BASENAME ,
                                                    computeDAXFile,
                                                    currentIteration );


    }


    public static void main( String[] args ){

        if (args.length == 3) {
            CreateIterativeDAX daxgen = new CreateIterativeDAX( Integer.parseInt( args[2] ) );
            String dax = daxgen.constructIterativeDAX( args[0], Integer.parseInt( args[1] ) );
            System.out.println( "Created iterative dax " + dax );

        } else {
            System.out.println("Usage: create-iterative-dax <dax-directory> <current-iteration> <max-iterations>");
        }
    }
}
