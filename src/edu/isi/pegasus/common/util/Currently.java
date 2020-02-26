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
package edu.isi.pegasus.common.util;

import java.text.*;
import java.util.*;
import java.util.regex.*;

/**
 * Create a common interface to handle obtaining string timestamps. The difficult part is to allow
 * for an ISO 8601 date formatting.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see java.util.Date
 * @see java.text.SimpleDateFormat
 */
public class Currently {
    /** This is used to format the time stamp. */
    private SimpleDateFormat m_formatter = null;

    /** Default time format, which is compact and has a timezone */
    public static final String DEFAULT_FORMAT = "yyyy.MM.dd HH:mm:ss zzz";

    /** Default ctor: Create a new instance with a default formatting string for timestamps. */
    public Currently() {
        m_formatter = new SimpleDateFormat(DEFAULT_FORMAT);
    }

    /**
     * Ctor: Create a new instance with a formatting string for time stamps.
     *
     * @param formatString complies to {@link java.text.SimpleDateFormat}.
     */
    public Currently(String formatString) {
        m_formatter = new SimpleDateFormat(formatString);
    }

    /**
     * Ctor: Create a new instance with a formatting string for time stamps.
     *
     * @param format is a description of the simple date format to use.
     */
    public Currently(SimpleDateFormat format) {
        m_formatter = (SimpleDateFormat) format.clone();
    }

    /**
     * Accessor: Obtains the default timestamp format for all queues.
     *
     * @return the currently active timestamp prefix format.
     * @see #setDateFormat( String )
     * @see #setDateFormat( SimpleDateFormat )
     */
    public SimpleDateFormat getDateFormat() {
        return m_formatter;
    }

    /**
     * Accessor: Sets the default timestamp format for all queues.
     *
     * @param format is the new timestamp prefix format.
     * @see #setDateFormat( SimpleDateFormat )
     * @see #getDateFormat()
     */
    public void setDateFormat(SimpleDateFormat format) {
        if (format != null) m_formatter = format;
    }

    /**
     * Accessor: Sets the default timestamp format for all queues.
     *
     * @param format is the new timestamp prefix format as a string.
     * @see #setDateFormat( String )
     * @see #getDateFormat()
     */
    public void setDateFormat(String format) {
        if (format != null) m_formatter = new SimpleDateFormat(format);
    }

    /**
     * Obtains the current time as formatted string according to the format option.
     *
     * @return the current time as formatted string.
     * @see #now( Date )
     */
    public String now() {
        return this.now(new Date());
    }

    /**
     * Obtains the current time as formatted string according to the format option.
     *
     * @param then is a timestamp expressed as Date.
     * @return the current time as formatted string.
     * @see #now()
     */
    public String now(Date then) {
        return this.m_formatter.format(then);
    }

    /** Store the regular expressions necessary to parse ISO timestamps. */
    private static final String c_expression[] = {
        "([12][0-9]{3})-([01][0-9])-([0123][0-9])(T([012][0-9]):([0-6][0-9]):([0-6][0-9])?([.,][0-9]+)?)?(Z|[-+][01][0-9]:?[0-5][0-9])?",
        "([12][0-9]{3})([01][0-9])([0123][0-9])(T?([012][0-9])([0-5][0-9])([0-6][0-9])?([.,][0-9]+)?)?(Z|[-+][01][0-9]:?[0-5][0-9])?"
    };

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern c_pattern[] = null;

    /**
     * Parses one of the ISO 8601 that it produces. Note, it will not parse the full range of ISO
     * timestamps.
     *
     * @param stamp is the textual timestamp representation.
     * @return a time or <code>null</code>, if unparsable.
     */
    public static Date parse(String stamp) {
        // initialize the compiled expressions once
        if (c_pattern == null) {
            c_pattern = new Pattern[c_expression.length];
            for (int i = 0; i < c_expression.length; ++i)
                c_pattern[i] = Pattern.compile(c_expression[i]);
        }

        // match against pattern
        for (int i = 0; i < c_expression.length; ++i) {
            Matcher m = c_pattern[i].matcher(stamp);
            if (m.matches()) {
                Calendar c = Calendar.getInstance();
                TimeZone z = TimeZone.getDefault();
                if (m.group(9) != null && m.group(9).length() > 0) {
                    boolean utc = (Character.toUpperCase(m.group(9).charAt(0)) == 'Z');
                    if (utc) z = TimeZone.getTimeZone("GMT+0");
                    else z = TimeZone.getTimeZone("GMT" + m.group(9));
                }

                c.setTimeZone(z);
                c.set(Calendar.YEAR, Integer.parseInt(m.group(1)));
                c.set(Calendar.MONTH, Integer.parseInt(m.group(2)) + (Calendar.JANUARY - 1));
                c.set(Calendar.DAY_OF_MONTH, Integer.parseInt(m.group(3)));

                if (m.group(4).length() > 0) {
                    c.set(Calendar.HOUR_OF_DAY, Integer.parseInt(m.group(5)));
                    c.set(Calendar.MINUTE, Integer.parseInt(m.group(6)));
                    if (m.group(7) != null && m.group(7).length() > 0)
                        c.set(Calendar.SECOND, Integer.parseInt(m.group(7)));
                    if (m.group(8) != null && m.group(8).length() > 1) {
                        String millis = m.group(8).substring(1);
                        while (millis.length() < 3) millis += "0";
                        millis = millis.substring(0, 3);
                        c.set(Calendar.MILLISECOND, Integer.parseInt(millis));
                    }
                }

                return c.getTime();
            }
        }

        // not found
        return null;
    }

    /**
     * Ignores any internal date format, and tries to show a complete date/timp stamp of the current
     * time in extended ISO 8601 format. UTC time (Zulu time) or a local timezone will be used. A
     * sample for UTC output is 2002-04-23T02:49:58Z A sample for local zone (CDT) is
     * 2002-04-22T21:49:58-05:00
     *
     * @param zuluTime returns a UTC formatted stamp, if true. Otherwise the time will be formatted
     *     according to the local zone.
     * @return an ISO 8601 formatted date and time representation for the current time in extended
     *     format without millisecond resolution
     * @see #iso8601( boolean, boolean, boolean, Date )
     */
    public static String iso8601(boolean zuluTime) {
        return Currently.iso8601(zuluTime, true, false, new Date());
    }

    /**
     * Ignores any internal date format, and tries to show a complete date/timp stamp in extended
     * ISO 8601 format. UTC time (Zulu time) or a local timezone will be used.
     *
     * <p>
     *
     * <table border=1>
     * <tr><th>zone</th><th>format</th><th>fraction</td><th>example</th></tr>
     * <tr><td>local</td><td>basic</td><td>integral</td><td>20020523T140427-0500</td></tr>
     * <tr><td>UTC</td><td>basic</td><td>integral</td><td>20020523190427Z</td></tr>
     * <tr><td>local</td><td>extd.</td><td>integral</td><td>2002-05-23T14:04:27-05:00</td></tr>
     * <tr><td>UTC</td><td>extd.</td><td>integral</td><td>2002-05-23T19:04:27Z</td></tr>
     * <tr><td>local</td><td>basic</td><td>millis</td><td>20020523T140427.166-0500</td></tr>
     * <tr><td>UTC</td><td>basic</td><td>millis</td><td>20020523190427.166Z</td></tr>
     * <tr><td>local</td><td>extd.</td><td>millis</td><td>2002-05-23T14:04:27.166-05:00</td></tr>
     * <tr><td>UTC</td><td>extd.</td><td>millis</td><td>2002-05-23T19:04:27.166Z</td></tr>
     * </table>
     *
     * <p>
     *
     * @param zuluTime returns a UTC formatted stamp, if true. Otherwise the time will be formatted
     *     according to the local zone. Local time should be prefixed with the 'T'.
     * @param extendedFormat will use the extended ISO 8601 format which separates the different
     *     timestamp items. If false, the basic format will be used. In UTC and basic format, the
     *     'T' separator will be omitted.
     * @param withMillis will put the millisecond extension into the timestamp. If false, the time
     *     will be without millisecond fraction. The separator is taken from {@link
     *     java.text.DecimalFormatSymbols#getMinusSign()}, which usually is a period or a comma.
     * @param now is a time stamp as Date.
     * @return an ISO 8601 formatted date and time representation for the given point in time.
     */
    public static String iso8601(
            boolean zuluTime, boolean extendedFormat, boolean withMillis, Date now) {
        Calendar c =
                Calendar.getInstance(
                        zuluTime ? TimeZone.getTimeZone("UTC") : TimeZone.getDefault());
        c.setTime(now);

        // set up formattting options
        DecimalFormat nf2 = new DecimalFormat("##00");
        DecimalFormat nf3 = new DecimalFormat("###000");
        DecimalFormat nf4 = new DecimalFormat("####0000");
        DecimalFormatSymbols dfs = nf2.getDecimalFormatSymbols();

        // allocate result string buffer
        int size = extendedFormat ? (zuluTime ? 25 : 30) : (zuluTime ? 21 : 25);
        if (!withMillis) size -= 4;
        StringBuffer result = new StringBuffer(size);

        result.append(nf4.format(c.get(Calendar.YEAR)));
        if (extendedFormat) result.append('-'); // position 5
        result.append(nf2.format(c.get(Calendar.MONTH) + 1));
        if (extendedFormat) result.append('-'); // position 8
        result.append(nf2.format(c.get(Calendar.DAY_OF_MONTH)));
        // generating UTC in basic format may leave out the 'T' separator
        if (extendedFormat || !zuluTime) result.append('T'); // position 11
        result.append(nf2.format(c.get(Calendar.HOUR_OF_DAY)));
        if (extendedFormat) result.append(':'); // position 14
        result.append(nf2.format(c.get(Calendar.MINUTE)));
        if (extendedFormat) result.append(':'); // position 17
        result.append(nf2.format(c.get(Calendar.SECOND)));

        if (withMillis) {
            // Though there is no explicit spec which allows a complete
            // timestamp with milliseconds, it is implied through two
            // levels, sigh. 5.3.4.2 allows decimal fractions with
            // time-only stamps that have a timezone. The intro of 5.4.2
            // allows 5.3.1.3.
            result.append(dfs.getDecimalSeparator()); // position 20
            result.append(nf3.format(c.get(Calendar.MILLISECOND)));
        }

        if (zuluTime) {
            // this is easy
            result.append('Z');
        } else {
            // time zone calculations
            int zone_offset = c.get(Calendar.ZONE_OFFSET) / 1000;
            int save_offset = c.get(Calendar.DST_OFFSET) / 1000;
            int diff = (zone_offset + save_offset) / 60;
            result.append(diff < 0 ? dfs.getMinusSign() : '+'); // position 24

            if (diff < 0) diff = Math.abs(diff);
            result.append(nf2.format(diff / 60));
            if (extendedFormat) result.append(':');
            result.append(nf2.format(diff % 60));
        }

        return result.toString();
    }
}
