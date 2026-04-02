/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.common.util;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;

/**
 * The default callback for the stream gobbler, that logs all the messages to a particular logging
 * level. By default all the messages are logged onto the DEBUG level.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DefaultStreamGobblerCallback implements StreamGobblerCallback {

    /** The level on which the messages are to be logged. */
    private int mLevel;

    /** The instance to the logger to log messages. */
    private LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param level the level on which to log.
     */
    public DefaultStreamGobblerCallback(int level) {
        // should do a sanity check on the levels
        mLevel = level;
        mLogger = LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler. The line is logged to
     * the level specified while initializing the class.
     *
     * @param line the line that is read.
     */
    public void work(String line) {
        mLogger.log(line, mLevel);
    }
}
