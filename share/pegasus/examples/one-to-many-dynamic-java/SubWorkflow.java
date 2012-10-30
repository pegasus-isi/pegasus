/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import edu.isi.pegasus.planner.dax.*;
import java.io.*;

public class SubWorkflow {

    /**
     * Create an example DIAMOND DAX
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java ADAG <site_handle> <pegasus_location> <inputfile list> <filename.dax>");
            System.exit(1);
        }

        try {
            Diamond(args[0], args[1], args[2]).writeToFile(args[3]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static ADAG Diamond(String site_handle, String pegasus_location, String inputfile) throws Exception {

    	ADAG dax = new ADAG("sub-workflow");
        try {
        	BufferedReader br = new BufferedReader(new FileReader(inputfile));
        

        	Executable analyze = new Executable("pegasus", "analyze", "4.0");
        	analyze.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        	analyze.setInstalled(true);
        	analyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", site_handle);

        	dax.addExecutable(analyze);
        	String line = null;
        	int jobid=0;
        	while ((line = br.readLine()) != null)   {
        		System.out.println("Line is "+line);
        	   String ifile[] = line.split(" ");
		   for (String i : ifile){
		       System.out.println("Tokens are "+i);
		   }

        	   edu.isi.pegasus.planner.dax.File fa = new edu.isi.pegasus.planner.dax.File(ifile[0]);
        	  	fa.addPhysicalFile(ifile[1], "TestCluster");
        	  	dax.addFile(fa);

        	  	// Add analyze job
        	  	jobid++;
        	  	edu.isi.pegasus.planner.dax.File fd= new edu.isi.pegasus.planner.dax.File(ifile[0]+".out");
        	  	Job j_analyze = new Job("j"+jobid, "pegasus", "analyze", "4.0");
        	  	j_analyze.addArgument("-a analyze -T 60 -i ").addArgument(fa);
        	  	j_analyze.addArgument("-o ").addArgument(fd);
        	  	j_analyze.uses(fa, edu.isi.pegasus.planner.dax.File.LINK.INPUT);
        	  	j_analyze.uses(fd, edu.isi.pegasus.planner.dax.File.LINK.OUTPUT);
        	  	dax.addJob(j_analyze);

        	}
  	  		//Close the input stream
  	  		br.close();
        } catch (IOException ioe){
        	System.err.println(ioe.getMessage());
        } catch (Exception e){
        	System.err.println(e.getMessage());       	
        }
        return dax;
    }
}
