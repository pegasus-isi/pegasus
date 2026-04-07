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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.io.StringWriter;
import org.griphyn.vdl.classes.Definitions;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dbschema.InMemorySchema;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ShowFullDiamondTest {

    @Test
    public void fullDiamondHarnessProducesDaxRenderableAsTextAndXml() throws Exception {
        Definitions diamond = CreateFullDiamond.create();
        Route route = new Route(new InMemorySchema(diamond));
        BookKeeper bookKeeper = route.requestLfn("f.d");
        ADAG dax = bookKeeper.getDAX("testing");

        StringWriter text = new StringWriter();
        dax.toString(text);

        StringWriter xml = new StringWriter();
        dax.toXML(xml, "");

        assertThat(diamond.getDefinitionCount(), is(8));
        assertThat(text.toString(), containsString("A"));
        assertThat(text.toString(), containsString("D"));
        assertThat(xml.toString(), containsString("<adag"));
        assertThat(xml.toString(), containsString("testing"));
    }
}
