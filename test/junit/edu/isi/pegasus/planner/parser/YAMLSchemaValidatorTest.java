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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.LoaderOptions;

/** @author Rajiv Mayani */
public class YAMLSchemaValidatorTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testGetInstanceReturnsSingletonAndKeepsFirstLoaderOptions() throws Exception {
        resetSingleton();
        LoaderOptions first = new LoaderOptions();
        first.setCodePointLimit(1234);
        LoaderOptions second = new LoaderOptions();
        second.setCodePointLimit(5678);

        YAMLSchemaValidator one = YAMLSchemaValidator.getInstance(first);
        YAMLSchemaValidator two = YAMLSchemaValidator.getInstance(second);

        assertThat("getInstance should return the singleton", two, sameInstance(one));
        assertThat(
                "Singleton should keep the loader options from the first initialization",
                getField(one, "mLoaderOptions"),
                sameInstance(first));
    }

    @Test
    public void testValidateReturnsSuccessForMatchingSchema() throws Exception {
        resetSingleton();
        YAMLSchemaValidator validator = YAMLSchemaValidator.getInstance(new LoaderOptions());
        Path schema = createSchemaFile();
        JsonNode node = new ObjectMapper(new YAMLFactory()).readTree("name: pegasus\n");

        try {
            YAMLSchemaValidationResult result =
                    validator.validate(node, schema.toFile(), "transformation");

            assertThat("Matching YAML should validate successfully", result.isSuccess(), is(true));
            assertThat(
                    "Error-message list should be initialized",
                    result.getErrorMessage(),
                    notNullValue());
            assertThat(
                    "Successful validation should have no errors",
                    result.getErrorMessage(),
                    empty());
        } finally {
            Files.deleteIfExists(schema);
        }
    }

    @Test
    public void testValidateReturnsErrorsForMissingRequiredField() throws Exception {
        resetSingleton();
        YAMLSchemaValidator validator = YAMLSchemaValidator.getInstance(new LoaderOptions());
        Path schema = createSchemaFile();
        JsonNode node = new ObjectMapper(new YAMLFactory()).readTree("{}");

        try {
            YAMLSchemaValidationResult result =
                    validator.validate(node, schema.toFile(), "transformation");

            assertThat(
                    "Missing required fields should fail validation",
                    result.isSuccess(),
                    is(false));
            assertThat(
                    "Error-message list should be initialized",
                    result.getErrorMessage(),
                    notNullValue());
            assertThat(
                    "Failed validation should report errors",
                    result.getErrorMessage(),
                    not(empty()));
        } finally {
            Files.deleteIfExists(schema);
        }
    }

    @Test
    public void testValidateInitializesErrorListEvenForSuccess() throws Exception {
        resetSingleton();
        YAMLSchemaValidator validator = YAMLSchemaValidator.getInstance(new LoaderOptions());
        Path schema = createSchemaFile();
        JsonNode node = new ObjectMapper(new YAMLFactory()).readTree("name: ok\n");

        try {
            YAMLSchemaValidationResult result = validator.validate(node, schema.toFile(), "site");

            assertThat(
                    "processValidation should always initialize the error-message list",
                    result.getErrorMessage(),
                    notNullValue());
        } finally {
            Files.deleteIfExists(schema);
        }
    }

    private Path createSchemaFile() throws Exception {
        Path schema = Files.createTempFile("yaml-schema-", ".json");
        String jsonSchema =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"required\": [\"name\"],\n"
                        + "  \"properties\": {\n"
                        + "    \"name\": {\"type\": \"string\"}\n"
                        + "  }\n"
                        + "}\n";
        Files.write(schema, jsonSchema.getBytes(StandardCharsets.UTF_8));
        return schema;
    }

    private void resetSingleton() throws Exception {
        ReflectionTestUtils.setField(YAMLSchemaValidator.class, "INSTANCE", null);
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }
}
