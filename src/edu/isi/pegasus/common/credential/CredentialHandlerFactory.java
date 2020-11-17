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
package edu.isi.pegasus.common.credential;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A factory class to load the appropriate type of Condor Style impelementations. This factory class
 * is different from other factories, in the sense that it must be instantiated first and intialized
 * first before calling out to any of the Factory methods.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CredentialHandlerFactory {

    /** The default package where the all the implementing classes are supposed to reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.common.credential.impl";
    //

    /** The name of the class implementing the credentials */
    private static final String CREDENTIALS_IMPLEMENTING_CLASS = "PegasusCredentials";

    private static final String X509_IMPLEMENTING_CLASS = "Proxy";

    private static final String IRODS_IMPLEMENTING_CLASS = "Irods";

    private static final String S3_IMPLEMENTING_CLASS = "S3CFG";

    private static final String BOTO_IMPLEMENTING_CLASS = "BotoConfig";

    private static final String GOOGLEP12_IMPLEMENTING_CLASS = "GoogleP12";

    private static final String SSH_IMPLEMENTING_CLASS = "Ssh";

    /**
     * Returns a table that maps, the credential types to the implementing classes.
     *
     * @return a Map indexed by Pegasus styles, and values as names of implementing classes.
     */
    private static Map<CredentialHandler.TYPE, String> implementingClassNameTable() {
        if (mImplementingClassNameTable == null) {
            mImplementingClassNameTable = new HashMap(3);
            mImplementingClassNameTable.put(
                    CredentialHandler.TYPE.credentials, CREDENTIALS_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(CredentialHandler.TYPE.x509, X509_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(CredentialHandler.TYPE.irods, IRODS_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(CredentialHandler.TYPE.s3, S3_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(CredentialHandler.TYPE.boto, BOTO_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(
                    CredentialHandler.TYPE.googlep12, GOOGLEP12_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put(CredentialHandler.TYPE.ssh, SSH_IMPLEMENTING_CLASS);
        }
        return mImplementingClassNameTable;
    }

    /**
     * A table that maps, Pegasus style keys to the names of the corresponding classes implementing
     * the CondorStyle interface.
     */
    private static Map mImplementingClassNameTable;

    /**
     * A table that maps, Pegasus style keys to appropriate classes implementing the
     * CredentialHandler interface
     */
    private Map<CredentialHandler.TYPE, CredentialHandler> mImplementingClassTable;

    /** A boolean indicating that the factory has been initialized. */
    private boolean mInitialized;

    /** Handle to the PegasusBag */
    private PegasusBag mBag;

    /** The default constructor. */
    public CredentialHandlerFactory() {
        mImplementingClassTable = new HashMap(3);
        mInitialized = false;
    }

    /**
     * Initializes the Factory. Loads all the implementations just once.
     *
     * @param bag the bag of initialization objects
     * @throws CredentialHandlerFactoryException that nests any error that might occur during the
     *     instantiation of the implementation.
     */
    public void initialize(PegasusBag bag) throws CredentialHandlerFactoryException {
        mBag = bag;

        // load all the implementations that correspond to the Pegasus style keys
        for (Iterator it = this.implementingClassNameTable().entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            CredentialHandler.TYPE type = (CredentialHandler.TYPE) entry.getKey();
            String className = (String) entry.getValue();

            // load via reflection. not required in this case though
            put(type, this.loadInstance(bag, className));
        }

        // we have successfully loaded all implementations
        mInitialized = true;
    }

    /**
     * This method loads the appropriate implementing CondorStyle as specified by the user at
     * runtime. The CondorStyle is initialized and returned.
     *
     * @param type the credential type that needs to be loaded.
     * @throws CredentialHandlerFactoryException that nests any error that might occur during the
     *     instantiation of the implementation.
     */
    public CredentialHandler loadInstance(CredentialHandler.TYPE type)
            throws CredentialHandlerFactoryException {

        // sanity checks first
        if (!mInitialized) {
            throw new CredentialHandlerFactoryException(
                    "Credential impelmentors needs to be initialized first before using");
        }

        // now just load from the implementing classes
        Object credentialHandler = this.get(type);
        if (credentialHandler == null) {
            // then load the class named type via reflection and register
            CredentialHandler handler = this.loadInstance(mBag, type.toString());
            this.put(type, handler);
        }

        return (CredentialHandler) credentialHandler;
    }

    /**
     * This method loads the appropriate CredentialHandler using reflection.
     *
     * @param bag the bag of initialization objects
     * @param className the name of the implementing class.
     * @return the instance of the class implementing this interface.
     * @throws CredentialHandlerFactoryException that nests any error that might occur during the
     *     instantiation of the implementation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    private CredentialHandler loadInstance(PegasusBag bag, String className)
            throws CredentialHandlerFactoryException {

        // sanity check
        PegasusProperties properties = bag.getPegasusProperties();
        if (properties == null) {
            throw new RuntimeException("Invalid properties passed");
        }
        if (className == null) {
            throw new RuntimeException("Invalid className specified");
        }

        // prepend the package name if classname is actually just a basename
        className =
                (className.indexOf('.') == -1)
                        ?
                        // pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className
                        :
                        // load directly
                        className;

        // try loading the class dynamically
        CredentialHandler credential = null;
        try {
            DynamicLoader dl = new DynamicLoader(className);
            credential = (CredentialHandler) dl.instantiate(new Object[0]);
            // initialize the loaded condor style
            credential.initialize(bag);
        } catch (Exception e) {
            throw new CredentialHandlerFactoryException(
                    "Instantiating Credential Handler ", className, e);
        }

        return credential;
    }

    /**
     * Returns the implementation from the implementing class table.
     *
     * @param type the credential handler type
     * @return implementation the class implementing that style, else null
     */
    private Object get(CredentialHandler.TYPE type) {
        return mImplementingClassTable.get(type);
    }

    /**
     * Inserts an entry into the implementing class table.
     *
     * @param type the credential handler type
     * @param implementation the class implementing that style.
     */
    private void put(CredentialHandler.TYPE type, CredentialHandler implementation) {
        mImplementingClassTable.put(type, implementation);
    }
}
