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

public class BlackDiamondDAX {

    /**
     * Create an example DIAMOND DAX
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java ADAG <site_handle> <pegasus_location> <filename.dax>");
            System.exit(1);
        }

        try {
            Diamond(args[0]).writeToFile(args[1]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static ADAG Diamond(String pegasus_location) throws Exception {

        java.io.File cwdFile = new java.io.File (".");
        String cwd = cwdFile.getCanonicalPath(); 

        ADAG dax = new ADAG("blackdiamond");

        File fa = new File("f.a");
        fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        File fb2 = new File("f.b2");
        File fc1 = new File("f.c1");
        File fc2 = new File("f.c2");
        File fd = new File("f.d");
	File fe1 = new File("f.e1");
	File fe2 = new File("f.e2");
        fd.setRegister(true);
	fe1.setRegister(true);
        fe2.setRegister(true);

	Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
        preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled( false );
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        Executable findrange = new Executable("pegasus", "findrange", "4.0");
        findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        findrange.setInstalled( false );
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

	Executable analyze = new Executable("pegasus", "analyze", "4.0");
        analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        analyze.setInstalled( false );
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

	Executable postanalyze = new Executable("pegasus", "post-analyze", "4.0");
        postanalyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        postanalyze.setInstalled( false );
        postanalyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");


        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);
	dax.addExecutable(postanalyze);

        // Add a preprocess job
        Job j1 = new Job("j1", "pegasus", "preprocess", "4.0");
        j1.addArgument("-a preprocess -T 10 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1);
        j1.addArgument(" ").addArgument(fb2);
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses(fb2, File.LINK.OUTPUT);
        dax.addJob(j1);

        // Add left Findrange job
        Job j2 = new Job("j2", "pegasus", "findrange", "4.0");
        j2.addArgument("-a findrange -T 10 -i ").addArgument(fb1);
        j2.addArgument("-o ").addArgument(fc1);
        j2.uses(fb1, File.LINK.INPUT);
        j2.uses(fc1, File.LINK.OUTPUT);
	j2.addProfile( "pegasus" , "label", "cluster1");
	
        dax.addJob(j2);

        // Add right Findrange job
        Job j3 = new Job("j3", "pegasus", "findrange", "4.0");
        j3.addArgument("-a findrange -T 10 -i ").addArgument(fb2);
        j3.addArgument("-o ").addArgument(fc2);
        j3.uses(fb2, File.LINK.INPUT);
        j3.uses(fc2, File.LINK.OUTPUT);
	j3.addProfile( "pegasus" , "label", "cluster1");
        dax.addJob(j3);

        // Add analyze job
        Job j4 = new Job("j4", "pegasus", "analyze", "4.0");
        j4.addArgument("-a analyze -T 10 -i ").addArgument(fc1);
        j4.addArgument(" ").addArgument(fc2);
        j4.addArgument("-o ").addArgument(fd);
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fc2, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
	j4.addProfile( "pegasus" , "label", "cluster1");
        dax.addJob(j4);

	//add left post-analyze job
        Job j5 = new Job("j5", "pegasus", "post-analyze", "4.0");
        j5.addArgument("-a findrange -T 10 -i ").addArgument(fd);
        j5.addArgument("-o ").addArgument(fe1);
        j5.uses(fd, File.LINK.INPUT);
        j5.uses(fe1, File.LINK.OUTPUT);
	j5.addProfile( "pegasus" , "label", "cluster2");
        dax.addJob(j5);

	//add right post-analyze job
        Job j6 = new Job("j6", "pegasus", "post-analyze", "4.0");
        j6.addArgument("-a findrange -T 10 -i ").addArgument(fd);
        j6.addArgument("-o ").addArgument(fe2);
        j6.uses(fd, File.LINK.INPUT);
        j6.uses(fe2, File.LINK.OUTPUT);
	j6.addProfile( "pegasus" , "label", "cluster2");
        dax.addJob(j6);

	
        dax.addDependency("j1", "j2");
        dax.addDependency("j1", "j3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
	dax.addDependency("j4", "j5");
	dax.addDependency("j4", "j6");

        return dax;
    }
}
