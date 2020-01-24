/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.vdl.directive;

import java.io.IOException;
import java.util.MissingResourceException;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * The base class for directives. Directives are a set of high-level common modules that facilitate
 * user interaction with the Chimera system.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public abstract class Directive {
    /** Logging instance */
    protected Logging m_logger;

    /** verbose logging mode */
    protected boolean m_verbose;

    /** properties instance */
    protected ChimeraProperties m_props;

    /** Constructor, initialize logging and properties instance */
    public Directive() throws IOException, MissingResourceException {
        m_logger = Logging.instance();
        m_verbose = false;
        m_props = ChimeraProperties.instance();
    }

    /**
     * set verbose mode
     *
     * @param v true for verbose mode, false otherwise
     */
    public void setVerbose(boolean v) {
        m_verbose = v;
    }

    /** get verbose mode */
    public boolean getVerbose() {
        return m_verbose;
    }

    /**
     * set logging instance
     *
     * @param logger the logging instance
     */
    public void setLogger(Logging logger) {
        if (logger != null) m_logger = logger;
    }

    /** get logging instance */
    public Logging getLogger() {
        return m_logger;
    }

    /** get properties instance */
    public ChimeraProperties getProperties() {
        return m_props;
    }

    /**
     * set properties instance
     *
     * @param props the chimera properties instance
     */
    public void setProperties(ChimeraProperties props) {
        if (props != null) m_props = props;
    }
}
