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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.invocation.Architecture;
import edu.isi.pegasus.planner.invocation.InvocationRecord;
import edu.isi.pegasus.planner.invocation.Usage;
import edu.isi.pegasus.planner.invocation.WorkingDir;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class InvocationParserTest {

    @Test
    public void invocationParserRoundTripsXmlLikeTheHarness() throws Exception {
        InvocationRecord record = new InvocationRecord();
        record.setVersion("2.1");
        record.setStart(new Date());
        record.setTransformation("pegasus::findrange");
        record.setUser("tester");
        record.setUID(1001);
        record.setGID(1001);
        record.setGroup("tester");
        record.setPID(4242);
        record.setHostAddress(InetAddress.getByName("127.0.0.1"));
        record.setHostname("localhost");
        record.setInterface("lo0");
        record.setWorkingDirectory(new WorkingDir("/tmp/work"));
        record.setUsage(new Usage());

        Architecture architecture = new Architecture();
        architecture.setSystemName("linux");
        architecture.setNodeName("worker.example.edu");
        architecture.setRelease("6.0");
        architecture.setMachine("x86_64");
        architecture.setValue("x86_64");
        record.setArchitecture(architecture);

        StringWriter xml = new StringWriter();
        record.toXML(xml, "", null);

        InvocationParser parser = new InvocationParser(InvocationRecord.SCHEMA_LOCATION);
        InvocationRecord parsed =
                parser.parse(
                        new ByteArrayInputStream(xml.toString().getBytes(StandardCharsets.UTF_8)));

        assertThat(parsed, notNullValue());
        assertThat(parsed.getTransformation(), is("pegasus::findrange"));
        assertThat(parsed.getUser(), is("tester"));
        assertThat(parsed.getWorkingDirectory().getValue(), is("/tmp/work"));
    }
}
