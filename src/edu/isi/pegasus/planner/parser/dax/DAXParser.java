/**
 * Copyright 2007-2011 University Of Southern California
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
package edu.isi.pegasus.planner.parser.dax;

/**
 * An interface for all the DAX Parsers
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface DAXParser {

    /**
     * Parse the DAX File
     * 
     * @param file 
     */
    public void parse(String file);

    /**
     * Set the DAXCallback for the parser to call out to.
     *
     * @param c the callback
     */
    public void setDAXCallback(Callback c);

    /**
     * Retuns the DAXCallback for the parser
     *
     * @return the callback
     */
    public Callback getDAXCallback();
}
