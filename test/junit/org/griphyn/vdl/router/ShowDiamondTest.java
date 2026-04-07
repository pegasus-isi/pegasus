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
package org.griphyn.vdl.router;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import org.griphyn.vdl.classes.Definitions;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dbschema.InMemorySchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** @author Rajiv Mayani */
public class ShowDiamondTest {

    @Test
    public void diamondHarnessRoundTripsThroughSerializationAndRouting(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        Definitions original = CreateDiamond.create();
        java.nio.file.Path file = tempDir.resolve("data.out");

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file.toFile()))) {
            oos.writeObject(original);
        }

        Definitions diamond;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file.toFile()))) {
            diamond = (Definitions) ois.readObject();
        }

        assertThat(diamond.getDefinitionCount(), is(original.getDefinitionCount()));

        Route route = new Route(new InMemorySchema(diamond));
        BookKeeper bookKeeper = route.requestLfn("f.d");
        ADAG dax = bookKeeper.getDAX("testing");

        assertThat(bookKeeper, notNullValue());
        assertThat(dax, notNullValue());
        assertThat(bookKeeper.toString(), containsString("f.d"));

        StringWriter xml = new StringWriter();
        dax.toXML(xml, "", null);
        assertThat(xml.toString(), containsString("<adag"));
        assertThat(xml.toString(), containsString("bottom"));
    }
}
