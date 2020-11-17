/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.catalog.site;

import edu.isi.pegasus.planner.catalog.CatalogException;

/**
 * Class to notify of failures. Exceptions are chained like the {@link java.sql.SQLException}
 * interface.
 *
 * <p>
 *
 * @author Jens-S. Vöckler, Karan Vahi
 * @see edu.isi.pegasus.planner.catalog.SiteCatalog
 */
public class SiteCatalogException extends CatalogException {

    /** Constructs a <code>SiteCatalogException</code> with no detail message. */
    public SiteCatalogException() {
        super();
    }

    /**
     * Constructs a <code>SiteCatalogException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public SiteCatalogException(String s) {
        super(s);
    }

    /**
     * Constructs a <code>SiteCatalogException</code> with the specified detailed message and a
     * cause.
     *
     * @param s is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public SiteCatalogException(String s, Throwable cause) {
        super(s, cause);
    }

    /**
     * Constructs a <code>SiteCatalogException</code> with the specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public SiteCatalogException(Throwable cause) {
        super(cause);
    }
}
