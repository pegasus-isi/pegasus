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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DAXParserTest {

    @Test
    public void testInterfaceShape() {
        assertThat(DAXParser.class.isInterface(), is(true));
        assertThat(DAXParser.class.getInterfaces().length, is(0));
    }

    @Test
    public void testMethodSignatures() throws Exception {
        Method parse = DAXParser.class.getMethod("parse", String.class);
        Method setCallback = DAXParser.class.getMethod("setDAXCallback", Callback.class);
        Method getCallback = DAXParser.class.getMethod("getDAXCallback");

        assertThat(parse.getReturnType(), is(Void.TYPE));
        assertThat(setCallback.getReturnType(), is(Void.TYPE));
        assertThat(getCallback.getReturnType(), is(Callback.class));

        assertThat(Modifier.isPublic(parse.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(parse.getModifiers()), is(true));
    }

    @Test
    public void testDeclaredMethodCount() {
        assertThat(DAXParser.class.getDeclaredMethods().length, is(3));
    }
}
