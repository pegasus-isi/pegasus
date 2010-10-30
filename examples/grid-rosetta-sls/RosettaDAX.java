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

import java.util.ArrayList;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;

import edu.isi.pegasus.planner.dax.*;

public class RosettaDAX {



    public void constructDAX(String daxfile, String site) {

        String hostname = hostname();

        try {
            java.io.File cwdFile = new java.io.File (".");
            String cwd = cwdFile.getCanonicalPath(); 

            // construct a dax object 
            ADAG dax = new ADAG("rosetta");

            // executables and transformations
            // including this in the dax is a new feature in 
            // 3.0. Earlier you had a standalone transformation catalog
            Executable exe = new Executable("rosetta.exe");
            
            // the executable is not installed on the remote sites, so 
            // pick it up from the local file system
            exe.setInstalled(false);
            exe.addPhysicalFile("gsiftp://" + hostname + cwd + "/rosetta.exe", site);
            
            // the dag needs to know about the executable to handle
            // transferrring 
            dax.addExecutable(exe);

            // all jobs depend on the flatfile databases
            List<File> inputs = new ArrayList<File>();
            recursiveAddToFileCollection(inputs,
                                         "minirosetta_database",
                                         "Rosetta Database",
                                         hostname,
                                         site);
            dax.addFiles(inputs); // for replica catalog

            // and some top level files
            File f1 = new File("design.resfile", File.LINK.INPUT);
            f1.addPhysicalFile("gsiftp://" + hostname + cwd + "/design.resfile", site);
            dax.addFile(f1);
            inputs.add(f1); // dependency for the job
            File f2 = new File("repack.resfile", File.LINK.INPUT);
            f2.addPhysicalFile("gsiftp://" + hostname + cwd + "/repack.resfile", site);
            dax.addFile(f2);
            inputs.add(f2); // dependency for the job

            java.io.File pdbDir = new java.io.File("pdbs/");
            String pdbs[] = pdbDir.list();
            for (int i = 0; i < pdbs.length; i++) {
                java.io.File pdb = new java.io.File("pdbs/" + pdbs[i]);
                if (pdb.isFile()) {
                    Job j = createJobFromPDB(dax, pdb, inputs, hostname, site);
                    dax.addJob(j);
                }
            }

            //write DAX to file
            dax.writeToFile(daxfile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private String hostname() {
        String hostname = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            byte[] ipAddr = addr.getAddress();
            hostname = addr.getCanonicalHostName();
        } catch (UnknownHostException e) {
            System.err.println("Unable to determine local hostname");
            System.exit(1);
        } 
        return hostname;
    }

    /*
     * This adds all the files in a directory to a set which can be used for job
     * data dependencies
    */ 
    private void recursiveAddToFileCollection(List<File> list, String dir, String desc, 
                                              String hostname, String site) {
        try {
            java.io.File d = new java.io.File(dir);
            String items[] = d.list();
            for (int i = 0; i < items.length; i++) {
                if (items[i].substring(0,1).equals(".")) {
                    continue;
                }
                java.io.File f = new java.io.File(dir + "/" + items[i]);
                if (f.isFile()) {
                    // File found, let's add it to the list
                    File input = new File(dir + "/" + items[i], File.LINK.INPUT);
                    input.addPhysicalFile("gsiftp://" + hostname + f.getAbsolutePath(), site);
                    list.add(input);
                }
                else {
                    recursiveAddToFileCollection(list, f.getPath(), desc, hostname, site);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private Job createJobFromPDB(ADAG dax, java.io.File pdb, List<File> inputs,
                                 String hostname, String site) {

        Job job = null;

        try {
            String id = pdb.getName();
            id = id.replaceAll(".pdb", "");

            job = new Job(id, "rosetta.exe");
            
            // general rosetta inputs (database, design, ...)
            job.uses(inputs, File.LINK.INPUT);

            // input pdb file
            File pdbFile = new File(pdb.getName());
            pdbFile.addPhysicalFile("gsiftp://" + hostname + pdb.getAbsolutePath(), site);
            job.uses(pdbFile, File.LINK.INPUT); // the job uses the file
            dax.addFile(pdbFile); // the dax needs to know about it to handle transfers
            
            // outputs
            File outFile = new File(pdb.getName() + ".score.sc");
            job.uses(outFile, File.LINK.OUTPUT); // the job uses the file

            // add the arguments to the job
            job.addArgument(" -in:file:s ");
            job.addArgument(pdbFile);
            job.addArgument(" -out:prefix " + pdb.getName() + ".");
            job.addArgument(" -database ./minirosetta_database");
            job.addArgument(" -linmem_ig 10");
            job.addArgument(" -nstruct 1");
            job.addArgument(" -pert_num 2");
            job.addArgument(" -inner_num 1");
            job.addArgument(" -jd2::ntrials 1");

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return job;
    }


    /**
     * Usage : RosettaDAX daxfile
     *
     * @param args the arguments passed
     */
    public static void main(String[] args) {
        RosettaDAX daxgen = new RosettaDAX();
        if (args.length == 2) {
            daxgen.constructDAX(args[0], args[1]);
        } else {
            System.out.println("Usage: RosettaDAX <outputdaxfile>");
        }
    }

}

