/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * An abstract base class for YAML Parser invoked from catalog implementations
 *
 * @author Karan Vahi
 */
public abstract class YAMLParser {

    /** Logger for logging the properties.. */
    protected final LogManager mLogger;

    /** Holder for various Pegasus properties.. */
    protected final PegasusProperties mProps;

    public YAMLParser(PegasusBag bag) {
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
    }

    /**
     * Validates a file against the Site Catalog Schema file
     *
     * @param f
     * @param schemaFile
     * @param catalogType
     * @return
     */
    protected boolean validate(File f, File schemaFile, String catalogType) {
        boolean validate = true;
        Reader reader = null;
        try {
            reader = new VariableExpansionReader(new FileReader(f));
        } catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        JsonNode root = null;
        try {
            root = mapper.readTree(reader);

        } catch (JacksonYAMLParseException e) {
            throw new ScannerException(e.getLocation().getLineNr(), parseError(e));
        } catch (Exception e) {
            throw new ScannerException("Error in loading the yaml file " + f, e);
        }
        if (root != null) {
            YAMLSchemaValidationResult result =
                    YAMLSchemaValidator.getInstance().validate(root, schemaFile, catalogType);

            // schema validation is done here.. in case of any validation error we throw the
            // result..
            if (!result.isSuccess()) {
                List<String> errors = result.getErrorMessage();
                StringBuilder errorResult = new StringBuilder();
                int i = 1;
                for (String error : errors) {
                    if (i > 1) {
                        errorResult.append(",");
                    }
                    errorResult.append("Error ").append(i++).append(":{");
                    errorResult.append(error).append("}");
                }
                throw new ScannerException(errorResult.toString());
            }
        }
        return validate;
    }

    /**
     * This method is used to extract the necessary information from the parsing exception
     *
     * @param e The parsing exception generated from the yaml.
     * @return String representing the line number and the problem is returned
     */
    protected String parseError(JacksonYAMLParseException e) {
        /*
        StringBuilder builder = new StringBuilder();
        builder.append("Problem in the line :" + (e.getProblemMark().getLine() + 1) + ", column:"
        		+ e.getProblemMark().getColumn() + " with tag "
        		+ e.getProblemMark().get_snippet().replaceAll("\\s", ""));
        return builder.toString();
                  */
        return e.toString();
    }

    /**
     * Remove potential leading and trailing quotes from a string.
     *
     * @param input is a string which may have leading and trailing quotes
     * @return a string that is either identical to the input, or a substring thereof.
     */
    public String niceString(String input) {
        // sanity
        if (input == null) {
            return input;
        }
        int l = input.length();
        if (l < 2) {
            return input;
        }

        // check for leading/trailing quotes
        if (input.charAt(0) == '"' && input.charAt(l - 1) == '"') {
            return input.substring(1, l - 1);
        } else {
            return input;
        }
    }
}
