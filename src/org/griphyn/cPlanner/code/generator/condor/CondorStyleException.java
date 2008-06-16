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

package org.griphyn.cPlanner.code.generator.condor;

import org.griphyn.cPlanner.code.CodeGeneratorException;

/**
 * A specific exception for the Condor Style generators.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class CondorStyleException extends CodeGeneratorException {

    /**
     * Constructs a <code>CondorStyleException</code> with no detail
     * message.
     */
    public CondorStyleException() {
        super();
    }

    /**
     * Constructs a <code>CondorStyleException</code> with the specified detailed
     * message.
     *
     * @param message is the detailled message.
     */
    public CondorStyleException(String message) {
        super(message);
    }

    /**
     * Constructs a <code>CondorStyleException</code> with the specified detailed
     * message and a cause.
     *
     * @param message is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public CondorStyleException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a <code>CondorStyleException</code> with the
     * specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public CondorStyleException(Throwable cause) {
        super(cause);
    }
}
