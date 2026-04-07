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
package edu.isi.pegasus.planner.catalog.work;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.WorkCatalog;
import java.io.PrintStream;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

/** Tests for the Database work catalog implementation. */
public class DatabaseTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    private static final class TestDatabase extends Database {
        TestDatabase() {
            super();
            this.mLogger = new NoOpLogManager();
            this.mLogger.setLevel(Level.DEBUG);
        }
    }

    @Test
    public void testDatabaseImplementsWorkCatalog() {
        assertThat(WorkCatalog.class.isAssignableFrom(Database.class), is(true));
    }

    @Test
    public void testDatabaseClassExists() {
        assertThat(Database.class, is(notNullValue()));
    }

    @Test
    public void testDatabaseIsInCorrectPackage() {
        assertThat(
                Database.class.getPackage().getName(), is("edu.isi.pegasus.planner.catalog.work"));
    }

    @Test
    public void testDefaultConstructorStartsClosed() {
        Database database = new TestDatabase();

        assertThat(database.isClosed(), is(true));
    }

    @Test
    public void testCloseOnUnconnectedDatabaseIsSafe() {
        Database database = new TestDatabase();

        database.close();

        assertThat(database.isClosed(), is(true));
    }

    @Test
    public void testConnectWithMissingUrlReturnsFalse() {
        Database database = new TestDatabase();
        Properties props = new Properties();
        props.setProperty("db.user", "pegasus");

        assertThat(database.connect(props), is(false));
        assertThat(database.isClosed(), is(true));
    }

    @Test
    public void testConnectWithInvalidDriverReturnsFalse() {
        Database database = new TestDatabase();
        Properties props = new Properties();
        props.setProperty("db.driver", "com.example.DoesNotExist");
        props.setProperty("db.url", "jdbc:invalid:test");

        assertThat(database.connect(props), is(false));
        assertThat(database.isClosed(), is(true));
    }

    @Test
    public void testConnectWithInvalidUrlReturnsFalse() {
        Database database = new TestDatabase();
        Properties props = new Properties();
        props.setProperty("db.url", "jdbc:invalid:test");

        assertThat(database.connect(props), is(false));
        assertThat(database.isClosed(), is(true));
    }
}
