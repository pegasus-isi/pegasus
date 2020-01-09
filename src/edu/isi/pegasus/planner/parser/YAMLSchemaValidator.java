/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.parser;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogKeywords;

import java.io.File;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This class if used to yaml object against the specified schema..
 *
 * @author Mukund Murrali
 *
 */
public class YAMLSchemaValidator {

    private static final YAMLSchemaValidator INSTANCE = new YAMLSchemaValidator();

    private YAMLSchemaValidator() {
    }

    /**
     * Singleton Class
     *
     */
    public static YAMLSchemaValidator getInstance() {
        return INSTANCE;
    }

    /**
     * This method is used to validate yaml data with schema and returns the
     * result.
     *
     * @param jsonNode    the root of the json node tree representing the yaml document
     * @param schemaFile  this represents the schema file for validation.
     * @param catalogType whether the transformation catalog or the site catalog 
     * @return YAMLSchemaValidationResult - A result representing the
     * success/failure along with the errors if any.
     */
    public YAMLSchemaValidationResult validate(JsonNode jsonNode, File schemaFile, String catalogType) {
        //need to pass URI path to ensure common.json gets resolved correctly
        URI schemaUri = schemaFile.toURI();
        ObjectMapper mapper =new ObjectMapper(new YAMLFactory());
        JsonSchemaFactory factory = JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)).objectMapper(mapper).build();
        JsonSchema schema = factory.getSchema(schemaUri);
        
        Set<ValidationMessage> messages = schema.validate(jsonNode);
        return processValidation( jsonNode, messages,  catalogType );
    }
    
    /**
     * This method is used to extract the result for any possible errors..
     *
     * @param node - TreeNode representing the yaml data
     * @param report - report generated from the json schema validation
     * @return YAMLSchemaValidationResult
     */
    private YAMLSchemaValidationResult processValidation(TreeNode node, Set<ValidationMessage> messages, String catalogType) {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();
        List<String> errorMessages = new LinkedList<String>();
        for(ValidationMessage message: messages ){
            errorMessages.add( message.getMessage() );
        }
        result.setSuccess( messages.isEmpty() );
        result.setErrorMessage(errorMessages);
        return result;
    }
    

    /**
     * This method is used to extract the result for any possible errors..
     *
     * @param node - TreeNode representing the yaml data
     * @param report - report generated from the json schema validation
     * @return YAMLSchemaValidationResult
     */
    /*
    private YAMLSchemaValidationResult processReport(TreeNode node, ProcessingReport report, String catalogType) {
        YAMLSchemaValidationResult result = new YAMLSchemaValidationResult();
        System.err.println(report);
        result.setSuccess(report.isSuccess());

        List<String> errorMessages = new LinkedList<String>();

        // for each of the error messages from the report, parse the data and return the
        // result
        for (final ProcessingMessage processingMessage : report) {
            if (processingMessage.getLogLevel().equals(LogLevel.ERROR)) {
                JsonNode jsonNode = processingMessage.asJson();
                String reason = jsonNode.get("keyword").asText();
                StringBuilder errorMessage = new StringBuilder();
                switch (reason) {
                    case "additionalProperties":
                        String unwanted = jsonNode.get("unwanted").toString();
                        errorMessage.append("Unknown fields " + unwanted + " present in ");
                        break;
                    case "required":
                        String missingFields = jsonNode.get("missing").toString();
                        errorMessage.append("Missing required fields " + missingFields + " in ");
                        break;
                    default:
                        errorMessage.append(processingMessage.getMessage()).append(" in ");

                }
                if (catalogType.equals("transformation")) {
                    populateTransformationErrorMessage(node, errorMessage, jsonNode);
                }
                if (catalogType.equals("site")) {
                    populateSiteErrorMessage(node, errorMessage, jsonNode);
                }
                errorMessages.add(errorMessage.toString());
            }
        }
        result.setErrorMessage(errorMessages);
        return result;
    }
    */

    /**
     * This is used to populate the error location or the node which causes the
     * error..
     *
     */
    private void populateTransformationErrorMessage(TreeNode node, StringBuilder errorMessage, JsonNode jsonNode) {
        /**
         * "instance":{"pointer":"/0/transformations/0/site/0"} this filed is
         * parsed to get the name of the transformation causing the issue..
         *
         */
        String path = jsonNode.get("instance").get("pointer").asText();
        String[] splitPaths = path.split("/");

        // this means some node inside the top level has a problem..
        if (splitPaths.length > 2) {

            // this represents the location of error..
            int location = Integer.parseInt(splitPaths[1]);

            // this filed represents the node causing the problem..
            String name = splitPaths[2];

            TreeNode nodeDetails = node.get(location).get(name);
            errorMessage.append(name);

            for (int i = 3; i < splitPaths.length; i++) {
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                if (i % 2 == 1) {
                    location = Integer.parseInt(splitPaths[i]);
                    TransformationCatalogKeywords reservedKey = TransformationCatalogKeywords.getReservedKey(name);
                    nodeDetails = nodeDetails.get(location);
                    switch (reservedKey) {
                        case TRANSFORMATIONS:
                            try {
                                errorMessage.append(" details - " + mapper.writeValueAsString(nodeDetails.get("namespace")));
                            } catch (JsonProcessingException e) {
                                errorMessage.append(" details - " + nodeDetails.get("namespace"));
                            }
                            break;
                        case SITES:
                            errorMessage.append(",Site - " + nodeDetails);
                            break;
                        case CONTAINERS:
                            try {
                                errorMessage.append(" details - " + mapper.writeValueAsString(nodeDetails.get("name")));
                            } catch (JsonProcessingException e) {
                                errorMessage.append(", details - " + nodeDetails.get("name"));
                            }
                            break;
                        default:
                            errorMessage.append(",property name -").append(reservedKey.getReservedName());
                    }
                } else {
                    name = splitPaths[i];
                    nodeDetails = nodeDetails.get(name);
                    if (i == splitPaths.length - 1) {
                        errorMessage.append(",property name - " + name);
                    }
                }
            }
        } else {
            errorMessage.append("top level error");
        }
    }

    /**
     * This is used to populate the error location or the node which causes the
     * error..
     *
     */
    private void populateSiteErrorMessage(TreeNode node, StringBuilder errorMessage, JsonNode jsonNode) {
        String path = jsonNode.get("instance").get("pointer").asText();
        String[] splitPaths = path.split("/");

        // this means some node inside the top level has a problem..
        if (splitPaths.length > 1) {
            try {
                // this represents the location of error..
                int location = Integer.parseInt(splitPaths[2]);

                TreeNode nodeDetails = node.get("site").get(location);
                errorMessage.append(nodeDetails);
            } catch (Exception e) {
                errorMessage.append(path);
            }

        } else {
            errorMessage.append("top level error");
        }
    }
}
