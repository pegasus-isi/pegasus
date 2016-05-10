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

public class HierarchicalDiamonds {

    /**
     * Create an example DIAMOND DAX
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java ADAG <pegasus_location>");
            System.exit(1);
        }

        try {
            TopDiamond(args[0]).writeToFile("top.dax");
            SubDiamond(args[0]).writeToFile("sub.dax");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static ADAG TopDiamond(String pegasus_location) throws Exception {

        java.io.File cwdFile = new java.io.File (".");
        String cwd = cwdFile.getCanonicalPath(); 

        ADAG dax = new ADAG("topdiamond");

        File fa = new File("f.a");
        fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        File fb2 = new File("f.b2");
        File fc1 = new File("f.c1");
        File fc2 = new File("f.c2");
        File fd = new File("f.d");
        fd.setRegister(true);

        Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
        preprocess.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(true);
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        Executable findrange = new Executable("pegasus", "findrange", "4.0");
        findrange.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        findrange.setInstalled(true);
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        Executable analyze = new Executable("pegasus", "analyze", "4.0");
        analyze.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        analyze.setInstalled(true);
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        // Add a preprocess job
        Job j1 = new Job("j1", "pegasus", "preprocess", "4.0");
        j1.addArgument("-a preprocess -T 120 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1);
        j1.addArgument(" ").addArgument(fb2);
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses(fb2, File.LINK.OUTPUT);
        dax.addJob(j1);

        // Add left Findrange job
        Job j2 = new Job("j2", "pegasus", "findrange", "4.0");
        j2.addArgument("-a findrange -T 120 -i ").addArgument(fb1);
        j2.addArgument("-o ").addArgument(fc1);
        j2.uses(fb1, File.LINK.INPUT);
        j2.uses(fc1, File.LINK.OUTPUT);
        dax.addJob(j2);

        // Add subdax
        File subdax = new File("sub.dax");
        subdax.addPhysicalFile("file://" + cwd + "/sub.dax", "local");
        dax.addFile(subdax);
        DAX j3 = new DAX("j3", subdax.getName());
        dax.addDAX(j3);

        // Add analyze job
        Job j4 = new Job("j4", "pegasus", "analyze", "4.0");
        j4.addArgument("-a analyze -T 120 -i ").addArgument(fc1);
        j4.addArgument("-o ").addArgument(fd);
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
        dax.addJob(j4);

        dax.addDependency("j1", "j2");
        dax.addDependency("j1", "j3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }
    
    private static ADAG SubDiamond(String pegasus_location) throws Exception {

        java.io.File cwdFile = new java.io.File (".");
        String cwd = cwdFile.getCanonicalPath(); 

        ADAG dax = new ADAG("subdiamond");

        File fa = new File("f.a");
        fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("sf.b1");
        File fb2 = new File("sf.b2");
        File fc1 = new File("sf.c1");
        File fc2 = new File("sf.c2");
        File fd = new File("sf.d");
        fd.setRegister(true);

        Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
        preprocess.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(true);
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        Executable findrange = new Executable("pegasus", "findrange", "4.0");
        findrange.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        findrange.setInstalled(true);
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        Executable analyze = new Executable("pegasus", "analyze", "4.0");
        analyze.setArchitecture(Executable.ARCH.X86_64).setOS(Executable.OS.LINUX);
        analyze.setInstalled(true);
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/pegasus-keg", "condorpool");

        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        // Add a preprocess job
        Job s1 = new Job("s1", "pegasus", "preprocess", "4.0");
        s1.addArgument("-a preprocess -T 120 -i ").addArgument(fa);
        s1.addArgument("-o ").addArgument(fb1);
        s1.addArgument(" ").addArgument(fb2);
        s1.uses(fa, File.LINK.INPUT);
        s1.uses(fb1, File.LINK.OUTPUT);
        s1.uses(fb2, File.LINK.OUTPUT);
        dax.addJob(s1);

        // Add left Findrange job
        Job s2 = new Job("s2", "pegasus", "findrange", "4.0");
        s2.addArgument("-a findrange -T 120 -i ").addArgument(fb1);
        s2.addArgument("-o ").addArgument(fc1);
        s2.uses(fb1, File.LINK.INPUT);
        s2.uses(fc1, File.LINK.OUTPUT);
        dax.addJob(s2);

        // Add right Findrange job
        Job s3 = new Job("s3", "pegasus", "findrange", "4.0");
        s3.addArgument("-a findrange -T 120 -i ").addArgument(fb2);
        s3.addArgument("-o ").addArgument(fc2);
        s3.uses(fb2, File.LINK.INPUT);
        s3.uses(fc2, File.LINK.OUTPUT);
        dax.addJob(s3);

        // Add analyze job
        Job s4 = new Job("s4", "pegasus", "analyze", "4.0");
        s4.addArgument("-a analyze -T 120 -i ").addArgument(fc1);
        s4.addArgument(" ").addArgument(fc2);
        s4.addArgument("-o ").addArgument(fd);
        s4.uses(fc1, File.LINK.INPUT);
        s4.uses(fc2, File.LINK.INPUT);
        s4.uses(fd, File.LINK.OUTPUT);
        dax.addJob(s4);

        dax.addDependency("s1", "s2");
        dax.addDependency("s1", "s3");
        dax.addDependency("s2", "s4");
        dax.addDependency("s3", "s4");
        return dax;
    }
}
