import edu.isi.pegasus.planner.dax.*;

public class split {

    public static void main(String[] args) {
        ADAG dax = new ADAG("split");

        File webpage = new File("pegasus.html");

        Job curl = new Job("ID1", "curl");
        curl.addArgument("-o").addArgument(webpage);
        curl.addArgument("http://pegasus.isi.edu");
        curl.uses(webpage, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        dax.addJob(curl);

        Job split = new Job("ID2", "split");
        split.addArgument("-l 100");
        split.addArgument("-a 1");
        split.addArgument(webpage);
        split.addArgument("part.");
        split.uses(webpage, File.LINK.INPUT, File.TRANSFER.FALSE, false);
        dax.addJob(split);

        dax.addDependency(curl, split);

        String[] array = new String[] { "a","b","c","d" };
        for (String c : array) {
            File part = new File("part." + c);
            split.uses(part, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);

            File count = new File("count.txt." + c);

            Job wc = new Job("ID"+c, "wc");
            wc.addArgument("-l");
            wc.addArgument(part);
            wc.setStdout(count);
            wc.uses(part, File.LINK.INPUT);
            wc.uses(count, File.LINK.OUTPUT, File.TRANSFER.TRUE, false);
            dax.addJob(wc);

            dax.addDependency(split, wc);
        }

        dax.writeToSTDOUT();
    }
}
