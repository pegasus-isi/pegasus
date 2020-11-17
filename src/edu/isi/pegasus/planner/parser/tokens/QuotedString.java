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
package edu.isi.pegasus.planner.parser.tokens;

/**
 * Class to capture the content within a quoted string.
 *
 * @author Jens Voeckler
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class QuotedString implements Token {

    /** This instance variable captures the token value for the quoted string. */
    private String m_value;

    /**
     * Initializes an instance of a quoted string.
     *
     * @param tokenValue is the string content to remember.
     */
    public QuotedString(String tokenValue) {
        m_value = tokenValue;
    }

    /**
     * Obtains the token value of a given string token.
     *
     * @return the token value.
     */
    public String getValue() {
        return this.m_value;
    }
}
