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
package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.common.util.FactoryException;

/**
 * Class to notify of failures while instantiating Transfer Implementations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TransferImplementationFactoryException extends FactoryException {

    /** The default classname that is associated with the exception. */
    public static final String DEFAULT_NAME = "Transfer Implementation";

    /**
     * Constructs a <code>TransferImplementationFactoryException</code> with no detail message. The
     * associated classname is set to value specified by <code>DEFAULT_NAME</code>.
     *
     * @param msg the detailed message.
     * @see #DEFAULT_NAME
     */
    public TransferImplementationFactoryException(String msg) {
        super(msg);
        mClassname = this.DEFAULT_NAME;
    }

    /**
     * Constructs a <code>TransferImplementationFactoryException</code> with the specified detailed
     * message.
     *
     * @param msg is the detailed message.
     * @param classname the name of class that was trying to be instantiated or some other signifier
     *     like module name.
     */
    public TransferImplementationFactoryException(String msg, String classname) {
        super(msg, classname);
    }

    /**
     * Constructs a <code>TransferImplementationFactoryException</code> with the specified detailed
     * message and a cause. The associated classname is set to value specified by <code>DEFAULT_NAME
     * </code>.
     *
     * @param msg is the detailed message that is to be logged.
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     * @see #DEFAULT_NAME
     */
    public TransferImplementationFactoryException(String msg, Throwable cause) {
        super(msg, cause);
        mClassname = this.DEFAULT_NAME;
    }

    /**
     * Constructs a <code>TransferImplementationFactoryException</code> with the specified detailed
     * message and a cause.
     *
     * @param msg is the detailed message that is to be logged.
     * @param classname the name of class that was trying to be instantiated.
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public TransferImplementationFactoryException(String msg, String classname, Throwable cause) {
        super(msg, cause);
        mClassname = classname;
    }
}
