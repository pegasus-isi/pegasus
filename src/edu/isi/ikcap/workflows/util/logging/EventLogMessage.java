package edu.isi.ikcap.workflows.util.logging;

/*
 * The following is the original copy right from gov.lbl.netlogger.LogMessage
 * Copyright (c) 2004, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy).  All rights reserved.
 */

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.locks.*;

/**
 * This is a modification of gov.lbl.netlogger.LogMessage
 *
 * <p>This class lets you easily construct a set of typed (name, value) pairs that formats itself as
 * a CEDPS Best Practices log message.
 *
 * <p>Name and value pairs are added into the record with add() methods which, by virtue of
 * returning the newly modified LogMessage instance, can be chained together.
 *
 * <p>The user can set the timestamp to something other than the time of the call by calling
 * setTimeStamp{Millis,Nanos}() as part of the chain.
 *
 * <p>To format the message, call toString(). The output format is <a
 * href="http://www.cedps.net/wiki/index.php/LoggingBestPractices">CEDPS "Best Practices"
 * format</a>.
 *
 * <p>Since the addition of the nanosecond timestamp (which is rounded down to microseconds, and no
 * I don't want to discuss it), this class requires Java 1.5
 *
 * @author Dan Gunter dkgunter@lbl.gov
 * @author Wolfgang Hoschek whoschek@lbl.gov
 * @author Paul Groth pgroth@isi.edu
 */
public class EventLogMessage {

    // Variables
    private final StringBuffer buf =
            new StringBuffer(256); // set initial capacity for efficiency/memory trade-off

    private static long micro0, nano0, micro1;
    private long micro2;

    // Static Variables
    private static String timeString = null;
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final GregorianCalendar calendar =
            new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    private static Lock timeStringLock = new ReentrantLock();

    // Constants
    public static final String APPENDER = "KEYVALUE";

    public static final String EVENT_KW = "event";
    public static final String DATE_KW = "ts";
    public static final String FAKE_DATE = "1999-01-01T11:59:59.999999Z";
    private final int dateStart = DATE_KW.length() + 1;
    private final int dateEnd = dateStart + FAKE_DATE.length() - 8;
    private final int usecStart = dateEnd + 1;

    @SuppressWarnings("unused")
    private final int usecEnd = dateEnd + 7;

    public static final String LEVEL_KW = "level=";

    private static final char[] DIGIT = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static Escape escape = new Escape("\"", '\\', false);

    /**
     * Create a new LogMessage at the current time with a given event name.
     *
     * <p>The timestamp is set at creation time, but can be changed later with <code>
     * setTimeStampMillis</code> or <code>setTimeStampNanos</code>.
     *
     * @param eventName Name of this logging event.
     * @see #setTimeStampMillis
     * @see #setTimeStampNanos
     */
    protected EventLogMessage(String eventName) {
        add(DATE_KW, FAKE_DATE);
        add(EVENT_KW, eventName);
        long nano1 = System.nanoTime();
        /* calculate timestamp in microseconds */
        micro2 = (nano1 - nano0) / 1000 + micro0;
    }

    /**
     * Add a string.
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage add(String key, String value) {
        buf.append(key);
        buf.append("=");
        buf.append(value);
        buf.append(" ");
        return this;
    }

    /**
     * Add a string. Strings automatically have quotes around them and are escaped
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage addWQ(String key, String value) {
        buf.append(key);
        buf.append("=");
        buf.append("\"");
        buf.append(escape.escape(value));
        buf.append("\"");
        buf.append(" ");
        return this;
    }

    /**
     * Add an int.
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage add(String key, int value) {
        buf.append(key);
        buf.append("=");
        buf.append(value);
        buf.append(" ");
        return this;
    }

    /**
     * Add a long.
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage add(String key, long value) {
        buf.append(key);
        buf.append("=");
        buf.append(value);
        buf.append(" ");
        return this;
    }

    /**
     * Add a float.
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage add(String key, float value) {
        buf.append(key);
        buf.append("=");
        buf.append(value);
        buf.append(" ");
        return this;
    }

    /**
     * Add a double.
     *
     * @return Self-reference, so calls can be chained
     */
    public EventLogMessage add(String key, double value) {
        buf.append(key);
        buf.append("=");
        buf.append(value);
        buf.append(" ");
        return this;
    }

    /**
     * Add a key,value pair The result looks like: key = (pairKey, pairValue)
     *
     * @param key
     * @param pairKey
     * @param pairValue
     */
    public EventLogMessage addPair(String key, String pairKey, String pairValue) {
        buf.append(key);
        buf.append("=");
        buf.append("(");
        buf.append(pairKey);
        buf.append(",");
        buf.append(pairValue);
        buf.append(")");
        buf.append(" ");
        return this;
    }

    public EventLogMessage addTime(String key, long timeInMillis) {
        buf.append(key);
        buf.append("=");
        buf.append(format.format(new Date(timeInMillis)));
        buf.append(" ");
        return this;
    }

    /**
     * Add a Map to the buffer. Represents maps as a series of (key, value) in quotes. The method
     * assumes that both keys and values toString method returns a "nice" string representation. The
     * method escapes all quotes
     *
     * @param key
     * @param map
     * @return
     */
    public EventLogMessage addMap(String key, Map map) {
        buf.append(key);
        buf.append("=");
        buf.append("\"");
        StringBuffer ps = new StringBuffer();
        Set<Map.Entry> pairs = map.entrySet();
        for (Map.Entry x : pairs) {
            ps.append("(");
            ps.append(x.getKey().toString());
            if (x.getValue() != null) {
                ps.append(",");
                ps.append(x.getValue().toString());
            }
            ps.append(")");
        }
        buf.append(escape.escape(ps.toString()));
        buf.append("\"");
        buf.append(" ");
        return this;
    }

    public EventLogMessage addList(String key, List list) {
        buf.append(key);
        buf.append("=");
        buf.append("\"");
        int size = list.size();
        for (int i = 0; i < size; i++) {
            buf.append(escape.escape(list.get(i).toString()));
            if (i != size - 1) {
                buf.append(",");
            }
        }
        buf.append("\"");
        buf.append(" ");
        return this;
    }

    /**
     * Add an natural language message to the log message. The string is put in quotes and is
     * escaped
     *
     * @param msg
     * @return
     */
    public EventLogMessage addMsg(String msg) {
        buf.append(LoggingKeys.MSG);
        buf.append("=");
        buf.append("\"");
        buf.append(escape.escape(msg));
        buf.append("\"");
        buf.append(" ");
        return this;
    }

    /**
     * Set the timestamp from milliseconds returned by System.currentTimeMillis().
     *
     * @return 'this' so we can chain
     */
    public EventLogMessage setTimeStampMillis(long millis) {
        micro2 = millis * 1000;
        return this;
    }

    /**
     * Set the timestamp from nanoseconds returned by System.nanoTime().
     *
     * @return 'this' so we can chain
     */
    public EventLogMessage setTimeStampNanos(long nano1) {
        micro2 = (nano1 - nano0) / 1000 + micro0;
        return this;
    }

    /**
     * Format a message in CEDPS Best Practices format.
     *
     * @return Formatted message string
     * @see <a href="http://www.cedps.net/wiki/index.php/LoggingBestPractices">CEDPS "Best
     *     Practices" format</a>
     */
    public String toString() {
        if (micro2 > 0) {
            addTimeStamp();
            micro2 = 0;
        }
        return buf.toString();
    }

    /** Add a timestamp to the message. */
    private void addTimeStamp() {
        // re-use or re-set whole seconds
        if (micro2 / 1000000L != micro1 / 1000000L) {
            timeStringLock.lock();
            timeString = format.format(new Date(micro2 / 1000L));
            timeStringLock.unlock();
            micro1 = micro2;
        }
        buf.replace(dateStart, dateEnd, timeString);

        // add fractional time (microseconds)
        long div, frac;
        int i;
        frac = micro2 % 1000000L;
        for (i = 0, div = 100000L; i < 6; div = div / 10, i++) {
            long n = frac / div;
            buf.setCharAt(usecStart + i, DIGIT[(int) n]);
            frac -= n * div;
        }
    }

    // ===============================================================
    // Log4J compatibility (contributed by Wolfgang Hoschek)
    // ===============================================================

    /**
     * Static class initializer.
     *
     * <p>Make it so that log4j.jar is a compile time requirement, but not a runtime requirement
     */
    static {
        try {
            // check if log4j is present
            Class.forName("org.apache.log4j.spi.Filter");
            // executed only if log4j is present
            Log4jFilter.init();
        } catch (ClassNotFoundException e) {
            // This warning might mess up daemon processes,
            // so it's commented out by default
            System.err.println(
                    "Warning: Cannot find log4j "
                            + "(org.apache.log4j.spi.Filter), "
                            + "continuing..");
        }
        // set calendar of formatter: otherwise no UTC!!
        format.setCalendar(calendar);
        // init base nanosecond and millisecond time
        long ms = System.currentTimeMillis();
        nano0 = System.nanoTime();
        micro0 = micro1 = ms * 1000L;
        timeString = format.format(new Date(ms));
    }

    /** In log4j, ignore all messages not specifically directed at this appender. */
    private static final class Log4jFilter extends org.apache.log4j.spi.Filter {

        public static void init() {
            Enumeration loggers =
                    org.apache.log4j.Logger.getRootLogger()
                            .getLoggerRepository()
                            .getCurrentLoggers();
            while (loggers.hasMoreElements()) {
                org.apache.log4j.Logger logger = (org.apache.log4j.Logger) loggers.nextElement();
                if (logger.getAppender(APPENDER) != null) {
                    logger.getAppender(APPENDER).addFilter(new Log4jFilter());
                }
            }
        }

        public int decide(org.apache.log4j.spi.LoggingEvent event) {
            if (event.getMessage() instanceof EventLogMessage) {
                return NEUTRAL; // let message pass through
            } else {
                return DENY; // ignore all non-netlogger messages
            }
        }
    }
}
