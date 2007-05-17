package org.griphyn.cPlanner.common;

/**
 * This interface defines the callback calls that are called from within the
 * StreamGobbler while working on a stream.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */
public interface StreamGobblerCallback {

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler.
     *
     * @param line   the line that is read.
     */
    public void work(String line);

}