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


/**
 * Class to notify of failures. Exceptions are chained like the {@link java.sql.SQLException}
 * interface.
 *
 * <p>Here is a fragment of code to chain exceptions for later throwing:
 *
 * <p>
 *
 * <pre>
 * CatalogException rce = null;
 * ... some loop code ... {
 *   ...
 *   if ( exception triggered ) {
 *     if ( rce == null ) rce = new CatalogException( reason );
 *     else rce.setNextException( new CatalogException(reason) );
 *   ...
 * } ... loop end ...
 * if ( rce != null ) throw rce;
 * </pre>
 *
 * Here is a fragment of code to unchain exceptions in the client:
 *
 * <p>
 *
 * <pre>
 * try {
 *   ... operation ...
 * } catch ( CatalogException rce ) {
 *   for ( ; rce != null; rce = rce.getNextException ) {
 *      ... do something with the payload ...
 *   }
 * }
 * </pre>
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 */
public class CatalogException
        // method A: no need to change interface, obsfuscated use, though
        extends java.lang.RuntimeException
// method B: needs API small change, but makes things clear.
// extends java.lang.Exception
{
    /** chains the next exception into line. */
    private CatalogException m_next_exception = null;

    /*
     * Constructs a <code>CatalogException</code> with no detail
     * message.
     */
    public CatalogException() {
        super();
        m_next_exception = null;
    }

    /**
     * Constructs a <code>CatalogException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public CatalogException(String s) {
        super(s);
        m_next_exception = null;
    }

    /**
     * Constructs a <code>CatalogException</code> with the specified detailed message and a cause.
     *
     * @param s is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public CatalogException(String s, Throwable cause) {
        super(s, cause);
        m_next_exception = null;
    }

    /**
     * Constructs a <code>CatalogException</code> with the specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the {@link
     *     java.lang.Throwable#getCause()} method). A <code>null</code> value is permitted, and
     *     indicates that the cause is nonexistent or unknown.
     */
    public CatalogException(Throwable cause) {
        super(cause);
        m_next_exception = null;
    }

    /**
     * Retrieves the exception chained to this <code>CatalogException</code> object.
     *
     * @return the next <code>CatalogException</code> object in the chain; <code>null</code> if
     *     there are none.
     * @see #setNextException( CatalogException )
     */
    public CatalogException getNextException() {
        return m_next_exception;
    }

    /**
     * Adds an <code>CatalogException<code> object to the end of
     * the chain.
     *
     * @param ex the new exception that will be added to the end of the
     * <code>CatalogException</code> chain.
     * @see #getNextException()
     */
    public void setNextException(CatalogException ex) {
        if (m_next_exception == null) {
            m_next_exception = ex;
        } else {
            CatalogException temp, rce = m_next_exception;
            while ((temp = rce.getNextException()) != null) {
                rce = temp;
            }
            rce.setNextException(ex);
        }
    }
}
