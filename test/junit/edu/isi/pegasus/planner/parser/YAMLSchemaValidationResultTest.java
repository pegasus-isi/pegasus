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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class YAMLSchemaValidationResultTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testDefaults() {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();

        assertThat("Default success flag should be false", result.isSuccess(), is(false));
        assertThat(
                "Default error-message list should be null", result.getErrorMessage(), nullValue());
    }

    @Test
    public void testSettersAndGetters() {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();
        List<String> errors = Arrays.asList("first", "second");

        result.setSuccess(true);
        result.setErrorMessage(errors);

        assertThat("setSuccess should update the success flag", result.isSuccess(), is(true));
        assertThat(
                "Setter should store the provided list reference",
                result.getErrorMessage(),
                sameInstance(errors));
    }

    @Test
    public void testErrorMessageListIsLiveReference() {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();
        List<String> errors = new ArrayList<String>();
        errors.add("first");
        result.setErrorMessage(errors);

        errors.add("second");

        assertThat(
                "The stored error-message list should reflect later mutations to the same list",
                result.getErrorMessage(),
                is(Arrays.asList("first", "second")));
    }

    @Test
    public void testToStringFormatsCurrentStateAndRequiresErrorList() {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();
        result.setSuccess(true);
        result.setErrorMessage(Arrays.asList("error-one"));

        assertThat(
                "toString should render the current success flag and error messages",
                result.toString(),
                is("success = true\t error messages = [error-one]"));

        YAMLSchemaValidationResult empty = new YAMLSchemaValidationResult();
        assertThrows(
                NullPointerException.class,
                empty::toString,
                "Current toString implementation requires a non-null error-message list");
    }
}
