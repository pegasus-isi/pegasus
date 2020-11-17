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
package edu.isi.pegasus.planner.catalog;

import java.util.Properties;

/**
 * This interface create a common ancestor for all cataloging interfaces.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface Catalog {

    /** The default DB Driver properties prefix. */
    public static final String DB_ALL_PREFIX = "pegasus.catalog.*.db";

    /**
     * Establishes a link between the implementation and the thing the implementation is build upon.
     *
     * <p>FIXME: The cause for failure is lost without exceptions.
     *
     * @param props contains all necessary data to establish the link.
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(Properties props);

    /** Explicitely free resources before the garbage collection hits. */
    public void close();

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed();
}
