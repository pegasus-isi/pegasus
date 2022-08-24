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
 * Copyright 1999-2020 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package edu.isi.pegasus.planner.common;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties.PEGASUS_MODE;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for testing the PegasusConfiguration
 *
 * @author Karan Vahi
 */
public class PegasusConfigurationTest {
    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    public PegasusConfigurationTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = PegasusProperties.nonSingletonInstance();

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.planner.common.pegasus-configuration", "setup", "0");

        mLogger.logEventCompletion();
    }

    @Test
    public void testLogLevelForDevelopmentMode() {
        testComputeLogLevel(
                PEGASUS_MODE.development, new PlannerOptions(), LogManager.DEBUG_MESSAGE_LEVEL);
    }

    @Test
    public void testLogLevelForDebugMode() {
        testComputeLogLevel(
                PEGASUS_MODE.debug, new PlannerOptions(), LogManager.TRACE_MESSAGE_LEVEL);
    }

    @Test
    public void testLogLevelForTutorialMode() {
        testComputeLogLevel(
                PEGASUS_MODE.tutorial, new PlannerOptions(), LogManager.WARNING_MESSAGE_LEVEL);
    }

    @Test
    public void testLogLevelForProductionMode() {
        testComputeLogLevel(
                PEGASUS_MODE.production, new PlannerOptions(), LogManager.WARNING_MESSAGE_LEVEL);
    }

    // PM-1883
    @Test
    public void testLogLevelForDevelopmentModeWithIncreasedVerbose() {
        PlannerOptions options = new PlannerOptions();
        options.incrementLogging(); // simulates -v
        testComputeLogLevel(PEGASUS_MODE.development, options, LogManager.TRACE_MESSAGE_LEVEL);
    }

    // PM-1883
    @Test
    public void testLogLevelForDevelopmentModeWithQuietOption() {
        PlannerOptions options = new PlannerOptions();
        options.decrementLogging(); // simulates -q
        options.decrementLogging();
        testComputeLogLevel(PEGASUS_MODE.development, options, LogManager.INFO_MESSAGE_LEVEL);
    }

    // PM-1883
    @Test
    public void testLogLevelForDevelopmentModeWithInsaneQuietOption() {
        PlannerOptions options = new PlannerOptions();
        for (int i = 0; i < 10; i++) {
            options.decrementLogging(); // simulates -q
        }
        testComputeLogLevel(PEGASUS_MODE.development, options, LogManager.FATAL_MESSAGE_LEVEL);
    }

    private void testComputeLogLevel(PEGASUS_MODE mode, PlannerOptions options, int expected) {
        PegasusConfiguration pc = new PegasusConfiguration(mLogger);
        int actual = pc.computeLogLevel(mode, options);
        assertEquals("Computed Log Level does not match", expected, actual);
    }

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }
}
