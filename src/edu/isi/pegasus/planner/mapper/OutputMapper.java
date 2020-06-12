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
package edu.isi.pegasus.planner.mapper;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.List;

/**
 * The interface that defines how to map the output files to a stage out site.
 *
 * @author vahi
 */
public interface OutputMapper extends Mapper {

    /** The version of the API of the Output Mapper */
    public static final String VERSION = "1.1";

    /**
     * Initializes the mappers.
     *
     * @param bag the bag of objects that is useful for initialization.
     * @param workflow the workflow refined so far.
     * @throws edu.isi.pegasus.planner.mapper.MapperException if unable to construct URL for any
     *     reason
     */
    public void initialize(PegasusBag bag, ADag workflow) throws MapperException;

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws edu.isi.pegasus.planner.mapper.MapperException if unable to construct URL for any
     *     reason
     */
    public NameValue map(String lfn, String site, FileServer.OPERATION operation)
            throws MapperException;

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @param existing indicates whether to create a new location/placement for a file, or rely on
     *     existing placement on the site.
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws edu.isi.pegasus.planner.mapper.MapperException if unable to construct URL for any
     *     reason
     */
    public NameValue map(String lfn, String site, FileServer.OPERATION operation, boolean existing)
            throws MapperException;

    /**
     * Maps a LFN to a location on the filsystem of a site and returns all the possible equivalent
     * externally accessible URL corresponding to that location. For example, if a file on the
     * filesystem is accessible via multiple file servers it should return externally accessible
     * URL's from all the File Servers on the site.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @return List of NameValue objects referring to mapped URL's along with their corresponding
     *     site information
     * @throws edu.isi.pegasus.planner.mapper.MapperException if unable to construct URL for any
     *     reason
     */
    public List<NameValue> mapAll(String lfn, String site, FileServer.OPERATION operation)
            throws MapperException;
}
