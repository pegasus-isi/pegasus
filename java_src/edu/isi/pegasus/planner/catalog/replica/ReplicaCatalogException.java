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
package edu.isi.pegasus.planner.catalog.replica;

import edu.isi.pegasus.planner.catalog.CatalogException;

/**
 * Class to notify of failures. Exceptions are chained like the {@link java.sql.SQLException}
 * interface.
 *
 * <p>
 *
 * @author Jens-S. Vöckler, Karan Vahi
 * @see org.griphyn.common.catalog.ReplicaCatalog
 */
public class ReplicaCatalogException extends CatalogException {
    /*
     * Constructs a <code>ReplicaCatalogException</code> with no detail
     * message.
     */
    public ReplicaCatalogException() {
        super();
    }

    /**
     * Constructs a <code>ReplicaCatalogException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public ReplicaCatalogException(String s) {
        super(s);
    }

    /**
     * Constructs a <code>ReplicaCatalogException</code> with the specified detailed message and a
     * cause.
     *
     * @param s is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public ReplicaCatalogException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs a <code>ReplicaCatalogException</code> with the specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public ReplicaCatalogException(Throwable cause) {
        super(cause);
    }
}
