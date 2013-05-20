import edu.isi.pegasus.planner.dax.*;

public class merge {

    public static void main(String[] args) {
        ADAG dax = new ADAG("merge");

        String[] dirs = new String[] { "/bin","/usr/bin","/usr/local/bin" };
        Job[] jobs = new Job[3];
        File[] files = new File[3];

        for (int i=0; i<3; i++) {
            String dir = dirs[i];
            Job ls = new Job("ID"+i, "ls");
            jobs[i] = ls;
            ls.addArgument("-l").addArgument(dir);
            File f = new File("bin_"+i+".txt");
            files[i] = f;
            ls.uses(f, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
            ls.setStdout(f);
            dax.addJob(ls);
        }

        Job cat = new Job("ID3", "cat");
        for (File f : files) {
            cat.addArgument(f);
            cat.uses(f, File.LINK.INPUT);
        }
        File output = new File("binaries.txt");
        cat.setStdout(output);
        cat.uses(output, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
        dax.addJob(cat);

        for (Job j : jobs) {
            dax.addDependency(j, cat);
        }

        dax.writeToSTDOUT();
    }
}

