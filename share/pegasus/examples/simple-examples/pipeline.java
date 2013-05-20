import edu.isi.pegasus.planner.dax.*;

public class pipeline {

    public static void main(String[] args) {
        ADAG dax = new ADAG("pipeline");

        File webpage = new File("pegasus.html");

        Job curl = new Job("ID1", "curl");
        curl.addArgument("-o").addArgument(webpage);
        curl.addArgument("http://pegasus.isi.edu");
        curl.uses(webpage, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        dax.addJob(curl);

        File count = new File("count.txt");

        Job wc = new Job("ID2", "wc");
        wc.addArgument("-l").addArgument(webpage);
        wc.setStdout(count);
        wc.uses(webpage, File.LINK.INPUT);
        wc.uses(count, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
        dax.addJob(wc);

        dax.addDependency(curl, wc);

        dax.writeToSTDOUT();
    }
}
