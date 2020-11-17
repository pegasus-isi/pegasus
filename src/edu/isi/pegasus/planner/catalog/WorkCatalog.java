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

import edu.isi.pegasus.planner.catalog.work.WorkCatalogException;

/**
 * The catalog interface to the Work Catalog, the erstwhile Work DB, that is populated by tailstatd
 * and associates.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface WorkCatalog extends Catalog {

    /** Prefix for the property subset to use with this catalog. */
    public static final String c_prefix = "pegasus.catalog.work";

    /** The DB Driver properties prefix. */
    public static final String DB_PREFIX = "pegasus.catalog.work.db";

    /** The version of the API */
    public static final String VERSION = "1.0";

    /**
     * Inserts a new mapping into the work catalog.
     *
     * @param basedir the base directory
     * @param vogroup the vo to which the user belongs to.
     * @param label the label in the DAX
     * @param run the run number.
     * @param creator the user who is running.
     * @param cTime the creation time of the DAX
     * @param mTime the modification time.
     * @param state the state of the workflow
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int insert(
            String basedir,
            String vogroup,
            String label,
            String run,
            String creator,
            java.util.Date cTime,
            java.util.Date mTime,
            int state)
            throws WorkCatalogException;
    /**
     * Deletes a mapping from the work catalog.
     *
     * @param basedir the base directory
     * @param vogroup the vo to which the user belongs to.
     * @param label the label in the DAX
     * @param run the run number.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int delete(String basedir, String vogroup, String label, String run)
            throws WorkCatalogException;
}
