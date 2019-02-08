/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.parser;

import java.io.LineNumberReader;

/**
 * This class is used to signal errors while scanning or parsing.
 *
 *
 * @see org.griphyn.cPlanner.classes.PoolConfigScanner
 * @see org.griphyn.cPlanner.classes.PoolConfigParser2
 *
 * @author Jens Voeckler
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public class ScannerException
        extends java.lang.RuntimeException {

    private int m_lineno;

    public ScannerException(LineNumberReader stream, String message) {
        super("line " + stream.getLineNumber() + ": " + message);
        this.m_lineno = stream.getLineNumber();
    }

    public ScannerException(int lineno, String message) {
        super("line " + lineno + ": " + message);
        this.m_lineno = lineno;
    }
    
    public ScannerException(String message) {
        super(message);
        this.m_lineno = -1;
    }
    
    public ScannerException(String message, Exception e) {
        super("message - " + message + ":", e);
        this.m_lineno = -1;
    }
    
    public ScannerException(int lineno, Exception e ) {
        super("line " + lineno + ": " , e );
        this.m_lineno = lineno;
    }

    public int getLineNumber() {
        return this.m_lineno;
    }
}
