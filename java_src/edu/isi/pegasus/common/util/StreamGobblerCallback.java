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

/**
 * This interface defines the callback calls that are called from within the StreamGobbler while
 * working on a stream.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface StreamGobblerCallback {

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler.
     *
     * @param line the line that is read.
     */
    public void work(String line);
}
