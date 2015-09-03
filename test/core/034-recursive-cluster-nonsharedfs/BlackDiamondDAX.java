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
        File fd1 = new File("f.d1");
	File fc3 = new File("f.c3");
        File fc4 = new File("f.c4");
        File fd2 = new File("f.d2");
	File fe1 = new File("f.e1");
	File fe2 = new File("f.e2");
        fd1.setRegister(true);
	fd2.setRegister(true);
	fe1.setRegister(true);
        fe2.setRegister(true);

	Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
        preprocess.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        preprocess.setInstalled( false );
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "local");

        Executable findrange = new Executable("pegasus", "findrange", "4.0");
        findrange.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        findrange.setInstalled( false );
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "local");

	Executable analyze = new Executable("pegasus", "analyze", "4.0");
        analyze.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        analyze.setInstalled( false );
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "local");

	Executable postanalyze = new Executable("pegasus", "post-analyze", "4.0");
        postanalyze.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        postanalyze.setInstalled( false );
        postanalyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "local");


        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);
	dax.addExecutable(postanalyze);

	//left based first label cluster
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
        Job j2l = new Job("j2l", "pegasus", "findrange", "4.0");
        j2l.addArgument("-a findrange -T 10 -i ").addArgument(fb1);
        j2l.addArgument("-o ").addArgument(fc1);
        j2l.uses(fb1, File.LINK.INPUT);
        j2l.uses(fc1, File.LINK.OUTPUT);
	j2l.addProfile( "pegasus" , "label", "cluster1");
        dax.addJob(j2l);

        // Add right Findrange job
        Job j3l = new Job("j3l", "pegasus", "findrange", "4.0");
        j3l.addArgument("-a findrange -T 10 -i ").addArgument(fb2);
        j3l.addArgument("-o ").addArgument(fc2);
        j3l.uses(fb2, File.LINK.INPUT);
        j3l.uses(fc2, File.LINK.OUTPUT);
	j3l.addProfile( "pegasus" , "label", "cluster1");
        dax.addJob(j3l);

        // Add analyze job
        Job j4l = new Job("j4l", "pegasus", "analyze", "4.0");
        j4l.addArgument("-a analyze -T 10 -i ").addArgument(fc1);
        j4l.addArgument(" ").addArgument(fc2);
        j4l.addArgument("-o ").addArgument(fd1);
        j4l.uses(fc1, File.LINK.INPUT);
        j4l.uses(fc2, File.LINK.INPUT);
        j4l.uses(fd1, File.LINK.OUTPUT);
	j4l.addProfile( "pegasus" , "label", "cluster1");
        dax.addJob(j4l);

	//second  parallel label cluster
	// Add left Findrange job                                                                                                                                                                                                                                                                                    
        Job j2r = new Job("j2r", "pegasus", "findrange", "4.0");
        j2r.addArgument("-a findrange -T 10 -i ").addArgument(fb1);
        j2r.addArgument("-o ").addArgument(fc3);
        j2r.uses(fb1, File.LINK.INPUT);
        j2r.uses(fc3, File.LINK.OUTPUT);
        j2r.addProfile( "pegasus" , "label", "cluster2");

        dax.addJob(j2r);

        // Add right Findrange job                                                                                                                                                                                                                                                                                    
        Job j3r = new Job("j3r", "pegasus", "findrange", "4.0");
        j3r.addArgument("-a findrange -T 10 -i ").addArgument(fb2);
        j3r.addArgument("-o ").addArgument(fc4);
        j3r.uses(fb2, File.LINK.INPUT);
        j3r.uses(fc4, File.LINK.OUTPUT);
        j3r.addProfile( "pegasus" , "label", "cluster2");
        dax.addJob(j3r);

        // Add analyze job                                                                                                                                                                                                                                                                                            
        Job j4r = new Job("j4r", "pegasus", "analyze", "4.0");
        j4r.addArgument("-a analyze -T 10 -i ").addArgument(fc3);
        j4r.addArgument(" ").addArgument(fc4);
        j4r.addArgument("-o ").addArgument(fd2);
        j4r.uses(fc3, File.LINK.INPUT);
        j4r.uses(fc4, File.LINK.INPUT);
        j4r.uses(fd2, File.LINK.OUTPUT);
        j4r.addProfile( "pegasus" , "label", "cluster2");
        dax.addJob(j4r);


	//add left post-analyze job
        Job j5 = new Job("j5", "pegasus", "post-analyze", "4.0");
        j5.addArgument("-a findrange -T 10 -i ").addArgument(fd1).addArgument( " " ).addArgument(fd2);
        j5.addArgument("-o ").addArgument(fe1);
        j5.uses(fd1, File.LINK.INPUT);
        j5.uses(fe1, File.LINK.OUTPUT);
	j5.addProfile( "pegasus" , "label", "cluster3");
        dax.addJob(j5);

	//add right post-analyze job
        Job j6 = new Job("j6", "pegasus", "post-analyze", "4.0");
        j6.addArgument("-a findrange -T 10 -i ").addArgument(fd1).addArgument( " " ).addArgument(fd2);
        j6.addArgument("-o ").addArgument(fe2);
	j6.uses(fd1, File.LINK.INPUT);
        j6.uses(fd2, File.LINK.INPUT);
        j6.uses(fe2, File.LINK.OUTPUT);
	j6.addProfile( "pegasus" , "label", "cluster3");
        dax.addJob(j6);
	
        dax.addDependency("j1", "j2l");
        dax.addDependency("j1", "j3l");
        dax.addDependency("j2l", "j4l");
        dax.addDependency("j3l", "j4l");

	dax.addDependency("j1", "j2r");
        dax.addDependency("j1", "j3r");
        dax.addDependency("j2r", "j4r");
        dax.addDependency("j3r", "j4r");

	dax.addDependency("j4l", "j5");
	dax.addDependency("j4l", "j6");
	dax.addDependency("j4r", "j5");
	dax.addDependency("j4r", "j6");

        return dax;
    }
}
