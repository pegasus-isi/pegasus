/**
 * Copyright 2007-2013 University Of Southern California
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CurrentlyTest {

    @Test
    public void testConstructorsAndDefaultFormat() {
        Currently defaults = new Currently();
        Currently fromPattern = new Currently("yyyy/MM/dd");
        SimpleDateFormat format = new SimpleDateFormat("HH:mm");
        Currently fromFormat = new Currently(format);

        assertThat(defaults.getDateFormat().toPattern(), is(Currently.DEFAULT_FORMAT));
        assertThat(fromPattern.getDateFormat().toPattern(), is("yyyy/MM/dd"));
        assertThat(fromFormat.getDateFormat().toPattern(), is("HH:mm"));

        format.applyPattern("ss");
        assertThat(fromFormat.getDateFormat().toPattern(), is("HH:mm"));
    }

    @Test
    public void testSetDateFormatUpdatesFormatterAndIgnoresNull() {
        Currently currently = new Currently("yyyy");
        SimpleDateFormat format = new SimpleDateFormat("MM");

        currently.setDateFormat(format);
        assertThat(currently.getDateFormat().toPattern(), is("MM"));

        currently.setDateFormat((SimpleDateFormat) null);
        assertThat(currently.getDateFormat().toPattern(), is("MM"));

        currently.setDateFormat("dd");
        assertThat(currently.getDateFormat().toPattern(), is("dd"));

        currently.setDateFormat((String) null);
        assertThat(currently.getDateFormat().toPattern(), is("dd"));
    }

    @Test
    public void testNowFormatsSuppliedDateWithCurrentPattern() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Currently currently = new Currently(format);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.clear();
        calendar.set(2024, Calendar.JANUARY, 2, 3, 4, 5);
        Date date = calendar.getTime();

        assertThat(currently.now(date), is(format.format(date)));
    }

    @Test
    public void testParseAcceptsExtendedAndBasicIso8601Formats() {
        Date extended = Currently.parse("2024-01-02T03:04:05.678Z");
        Date basic = Currently.parse("20240102T030405Z");

        assertThat(extended, is(org.hamcrest.Matchers.notNullValue()));
        assertThat(basic, is(org.hamcrest.Matchers.notNullValue()));

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(extended);
        assertThat(calendar.get(Calendar.YEAR), is(2024));
        assertThat(calendar.get(Calendar.MONTH), is(Calendar.JANUARY));
        assertThat(calendar.get(Calendar.DAY_OF_MONTH), is(2));
        assertThat(calendar.get(Calendar.HOUR_OF_DAY), is(3));
        assertThat(calendar.get(Calendar.MINUTE), is(4));
        assertThat(calendar.get(Calendar.SECOND), is(5));
        assertThat(calendar.get(Calendar.MILLISECOND), is(678));

        calendar.setTime(basic);
        assertThat(calendar.get(Calendar.YEAR), is(2024));
        assertThat(calendar.get(Calendar.MONTH), is(Calendar.JANUARY));
        assertThat(calendar.get(Calendar.DAY_OF_MONTH), is(2));
        assertThat(calendar.get(Calendar.HOUR_OF_DAY), is(3));
        assertThat(calendar.get(Calendar.MINUTE), is(4));
        assertThat(calendar.get(Calendar.SECOND), is(5));
    }

    @Test
    public void testParseRejectsInvalidInput() {
        assertThat(Currently.parse("not-a-timestamp"), is(nullValue()));
        assertThat(Currently.parse("2024-13-99T99:99:99Z"), is(nullValue()));
    }

    @Test
    public void testIso8601ProducesExpectedUtcStrings() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.clear();
        calendar.set(2024, Calendar.JANUARY, 2, 3, 4, 5);
        calendar.set(Calendar.MILLISECOND, 678);
        Date date = calendar.getTime();

        assertThat(Currently.iso8601(true, true, false, date), is("2024-01-02T03:04:05Z"));
        assertThat(Currently.iso8601(true, false, true, date), is("20240102030405.678Z"));
    }
}
