import edu.isi.pegasus.planner.dax.*;

public class process {

    public static void main(String[] args) {
        ADAG dax = new ADAG("process");

        File listing = new File("listing.txt");

        Job ls = new Job("ID1", "ls");
        ls.addArgument("-l").addArgument("/");
        ls.setStdout(listing);
        ls.uses(listing, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
        dax.addJob(ls);

        dax.writeToSTDOUT();
    }
}
