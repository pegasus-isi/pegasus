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
        if (args.length != 3) {
            System.out.println("Usage: java ADAG <site_handle> <pegasus_location> <filename.dax>");
            System.exit(1);
        }

        try {
            Diamond(args[0], args[1]).writeToFile(args[2]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static ADAG Diamond(String site_handle, String pegasus_location) throws Exception {
       
        java.io.File cwdFile = new java.io.File (".");
        String cwd = cwdFile.getCanonicalPath(); 
        
        ADAG dax = new ADAG("blackdiamond");

        File fa = new File("f.a");
        fa.addProfile("env", "FOO", "/usr/bar");
        fa.addProfile("globus", "walltime", "40");
        fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        fb1.addProfile("env", "GOO", "/usr/foo");
        fb1.addProfile("globus", "walltime", "40");
        dax.addFile(fb1);

        File fb2 = new File("f.b2");
        fb2.addProfile("env", "BAR", "/usr/goo");
        fb2.addProfile("globus", "walltime", "40");
        dax.addFile(fb2);

        File fc1 = new File("f.c1");
        dax.addFile(fc1);

        File fc2 = new File("f.c2");
        dax.addFile(fc2);

        File fd = new File("f.d");
        dax.addFile(fd);

        Executable preprocess = new Executable("pegasus", "preprocess", "1.0");
        preprocess.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(true);
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
        preprocess.addProfile(Profile.NAMESPACE.globus, "walltime", "120");

        Executable findrange = new Executable("pegasus", "findrange", "1.0");
        findrange.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        findrange.setInstalled(true);
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

        Executable analyze = new Executable("pegasus", "analyze", "1.0");
        analyze.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        analyze.setInstalled(true);
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        Transformation diamond = new Transformation("pegasus", "diamond", "1.0");
        diamond.uses(preprocess).uses(findrange).uses(analyze);
        diamond.uses(new File("config", File.LINK.INPUT));

        dax.addTransformation(diamond);

        Job j1 = new Job("j1", "pegasus", "preprocess", "1.0", "j1");
        j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1).addArgument(fb2);
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses(new File("f.b2"), File.LINK.OUTPUT);
        dax.addJob(j1);

        DAG j2 = new DAG("j2", "findrange.dag", "j2");
        j2.uses(new File("f.b1"), File.LINK.INPUT);
        j2.uses(new File("f.c1"), File.LINK.OUTPUT);
        dax.addDAG(j2);

        DAX j3 = new DAX("j3", "findrange.dax", "j3");
        j3.addArgument("--site ").addArgument("local");
        j3.uses(new File("f.b2"), File.LINK.INPUT);
        j3.uses(new File("f.c2"), File.LINK.OUTPUT);
        dax.addDAX(j3);

        Job j4 = new Job("j4", "pegasus", "analyze", "1.0");
        j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
        j4.addArgument(" ").addArgument(fc2);
        j4.addArgument("-o ").addArgument(fd);
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fc2, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
        dax.addJob(j4);

        dax.addDependency("j1", "j2", "1-2");
        dax.addDependency("j1", "j3", "1-3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }
}
