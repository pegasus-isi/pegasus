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

public class RootWorkflow {

    /**
     * Create an example DIAMOND DAX
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java ADAG <site_handle> <cluster pegasus location> <cluster software_location> <filename.dax>");
            System.exit(1);
        }

        try {
            Diamond(args[0], args[1], args[2]).writeToFile(args[3]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static ADAG Diamond(String site_handle, String cluster_pegasus_location, String cluster_software_location) throws Exception {

        java.io.File cwdFile = new java.io.File (".");
        String cwd = cwdFile.getCanonicalPath(); 

        ADAG dax = new ADAG("root-workflow");

        File input = new File("input");
        input.addPhysicalFile("file://" + cwd + "/input", "local");
        dax.addFile(input);

        File subdax = new File("sub-workflow.dax");
        subdax.addPhysicalFile("file://" + cwd + "/sub-workflow.dax", "local");
        dax.addFile(subdax);
        
       
        File fb2 = new File("f.b2");
        File fc1 = new File("f.c1");
        File fc2 = new File("f.c2");
        File fd = new File("f.d");
        fd.setRegister(true);

        Executable split = new Executable("linux", "split", "");
        split.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        split.setInstalled(true);
        split.addPhysicalFile("file://" + cluster_software_location + "/split", site_handle);
        dax.addExecutable(split);
        
        Executable generate = new Executable("workflow", "generate", "");
        generate.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        generate.setInstalled(true);
        generate.addPhysicalFile("file://" + System.getProperty("java.home") + "/bin/java", "local");
        dax.addExecutable(generate);
        
        // Add a preprocess job
       
        Job j_split = new Job("j1", "linux", "split", "");
        j_split.addArgument("-l 1").addArgument(input).addArgument("input. ");
        j_split.uses(input, File.LINK.INPUT);
        File ilist = new File("input_list");
        j_split.setStdout(ilist);
        dax.addJob(j_split);

        // Add left Findrange job
        Job j_generate = new Job("j2", "workflow", "generate", "");
        j_generate.addArgument("SubWorkflow");
        j_generate.addArgument(site_handle).addArgument(cluster_pegasus_location);
        j_generate.addArgument(ilist);
        j_generate.addArgument(cwd+"/"+subdax.getName());
        j_generate.uses(ilist, File.LINK.INPUT);
        j_generate.addProfile(Profile.NAMESPACE.env,"CLASSPATH",cwd+":"+System.getProperty("java.class.path"));
        dax.addJob(j_generate);

        // Add right Findrange job
        DAX j_subdax = new DAX("j3", subdax.getName());
        j_subdax.addArgument("--output-site local").addArgument("--basename sub-workflow");
        dax.addDAX(j_subdax);

        dax.addDependency("j1", "j2");
        dax.addDependency("j2", "j3");
        return dax;
    }
}
