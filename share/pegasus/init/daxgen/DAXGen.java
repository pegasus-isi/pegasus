import edu.isi.pegasus.planner.dax.*;

/* API Documentation: http://pegasus.isi.edu/documentation */
public class DAXGen {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: DAXGen DAXFILE");
            System.exit(1);
        }

        String daxfile = args[0];

        ADAG dax = new ADAG("{{name}}");

        // TODO Add some jobs
        /*
        Job j = new Job("ID1", "myexe");
        File a = new File("a.txt");
        File b = new File("b.txt");
        File c = new File("c.txt");
        j.addArgument("-i").addArgument(a);
        j.addArgument("-o").addArgument(b);
        j.addArgument("-o").addArgument(c);
        j.uses(a, File.LINK.INPUT, File.TRANSFER.TRUE, false);
        j.uses(b, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
        j.uses(c, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
        dax.addJob(j);
        */

        // TODO Add dependencies
        /*
        dax.addDependency(j, k);
        */

        dax.writeToFile(daxfile);
    }
}

