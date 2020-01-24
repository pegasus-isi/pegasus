/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.builder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.batch.model.ContainerProperties;
import software.amazon.awssdk.services.batch.model.Host;
import software.amazon.awssdk.services.batch.model.JobDefinitionType;
import software.amazon.awssdk.services.batch.model.KeyValuePair;
import software.amazon.awssdk.services.batch.model.MountPoint;
import software.amazon.awssdk.services.batch.model.RegisterJobDefinitionRequest;
import software.amazon.awssdk.services.batch.model.RetryStrategy;
import software.amazon.awssdk.services.batch.model.Ulimit;
import software.amazon.awssdk.services.batch.model.Volume;

/**
 * A Builder to get objects related to Registering a Job Definition The JSON specification can be
 * found in the AWS Batch documentation.
 *
 * <p>http://docs.aws.amazon.com/batch/latest/APIReference/API_RegisterJobDefinition.html
 *
 * <p>Sytax of JSON document parsed is below
 *
 * <pre>
 *  {
 * "containerProperties": {
 * "command": [ "string" ],
 * "environment": [
 * {
 * "name": "string",
 * "value": "string"
 * }
 * ],
 * "image": "string",
 * "jobRoleArn": "string",
 * "memory": number,
 * "mountPoints": [
 * {
 * "containerPath": "string",
 * "readOnly": boolean,
 * "sourceVolume": "string"
 * }
 * ],
 * "privileged": boolean,
 * "readonlyRootFilesystem": boolean,
 * "ulimits": [
 * {
 * "hardLimit": number,
 * "name": "string",
 * "softLimit": number
 * }
 * ],
 * "user": "string",
 * "vcpus": number,
 * "volumes": [
 * {
 * "host": {
 * "sourcePath": "string"
 * },
 * "name": "string"
 * }
 * ]
 * },
 * "jobDefinitionName": "string",
 * "parameters": {
 * "string" : "string"
 * },
 * "retryStrategy": {
 * "attempts": number
 * },
 * "type": "string"
 * }
 * </pre>
 *
 * @author Karan Vahi
 */
public class JobDefinition {

    public JobDefinition() {}

    /**
     * Parses and reads in a Register Job Definition request from a JSON documentation containing
     * the request in the same format as expected by AWS Batch HTTP specification.
     *
     * @param f
     * @return
     */
    public RegisterJobDefinitionRequest createRegisterJobDefinitionRequestFromHTTPSpec(
            File f, String name) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        try {
            input = new FileInputStream(f);
            JsonNode root = mapper.readTree(input);
            // System.out.println(  root );
            return this.createRegisterJobDefinitionRequest(root, name);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException("Unable to find file f " + f, ex);
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read json from file f " + f, ex);
        }
    }

    public List<RegisterJobDefinitionRequest> createRegisterJobDefinitionRequest(
            File f, String name) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        List<RegisterJobDefinitionRequest> ceRequests = new LinkedList();
        try {
            input = new FileInputStream(f);
            JsonNode root = mapper.readTree(input);
            JsonNode node = root;
            if (node.has("RegisterJobDefinition")) {
                node = node.get("RegisterJobDefinition");
                // System.out.println( node );
                if (node.isArray()) {
                    for (JsonNode ceNode : node) {
                        if (ceNode.has("input")) {
                            ceRequests.add(
                                    this.createRegisterJobDefinitionRequest(
                                            ceNode.get("input"), name));
                        }
                    }
                } else {
                    throw new RuntimeException(
                            "RegisterJobDefinition value should be of type array ");
                }
            }

        } catch (FileNotFoundException ex) {
            throw new RuntimeException("Unable to find file f " + f, ex);
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read json from file f " + f, ex);
        }
        return ceRequests;
    }

    /**
     * Populates the builder , with information from the node
     *
     * @param node the JSON node representing the node value
     */
    private RegisterJobDefinitionRequest createRegisterJobDefinitionRequest(
            final JsonNode node, String name) {
        /*

            {
                "containerProperties": {
                   "command": [ "string" ],
                   "environment": [
                      {
                         "name": "string",
                         "value": "string"
                      }
                   ],
                   "image": "string",
                   "jobRoleArn": "string",
                   "memory": number,
                   "mountPoints": [
                      {
                         "containerPath": "string",
                         "readOnly": boolean,
                         "sourceVolume": "string"
                      }
                   ],
                   "privileged": boolean,
                   "readonlyRootFilesystem": boolean,
                   "ulimits": [
                      {
                         "hardLimit": number,
                         "name": "string",
                         "softLimit": number
                      }
                   ],
                   "user": "string",
                   "vcpus": number,
                   "volumes": [
                      {
                         "host": {
                            "sourcePath": "string"
                         },
                         "name": "string"
                      }
                   ]
                },
                "jobDefinitionName": "string",
                "parameters": {
                   "string" : "string"
                },
                "retryStrategy": {
                   "attempts": number
                },
                "type": "string"
             }

        */
        RegisterJobDefinitionRequest.Builder builder = RegisterJobDefinitionRequest.builder();
        if (node.has("containerProperties")) {
            builder.containerProperties(createContainerProperties(node.get("containerProperties")));
        }
        if (node.has("jobDefinitionName")) {
            builder.jobDefinitionName(node.get("jobDefinitionName").asText());
        } else {
            builder.jobDefinitionName(name);
        }

        if (node.has("parameters")) {
            JsonNode params = node.get("parameters");
            // System.out.println( "TAGs are " + tags);
            Map<String, String> m = new HashMap();
            for (Iterator<Map.Entry<String, JsonNode>> it = params.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                m.put(entry.getKey(), entry.getValue().asText());
            }
            builder.parameters(m);
        }
        if (node.has("retryStrategy")) {
            JsonNode retries = node.get("retryStrategy");
            RetryStrategy.Builder rsBuilder = RetryStrategy.builder();
            if (retries.has("attempts")) {
                rsBuilder.attempts(retries.get("attempts").asInt());
            } else {
                throw new RuntimeException("retry strategy expects attribute attempts");
            }
            builder.retryStrategy(rsBuilder.build());
        }

        if (node.has("type")) {
            builder.type(node.get("type").asText());
        }
        // System.out.println( "YEAH" + builder.build() );
        return builder.build();
    }

    private ContainerProperties createContainerProperties(final JsonNode node) {
        ContainerProperties.Builder builder = ContainerProperties.builder();
        // System.out.println( node );
        /*
           {
                   "command": [ "string" ],
                   "environment": [
                      {
                         "name": "string",
                         "value": "string"
                      }
                   ],
                   "image": "string",
                   "jobRoleArn": "string",
                   "memory": number,
                   "mountPoints": [
                      {
                         "containerPath": "string",
                         "readOnly": boolean,
                         "sourceVolume": "string"
                      }
                   ],
                   "privileged": boolean,
                   "readonlyRootFilesystem": boolean,
                   "ulimits": [
                      {
                         "hardLimit": number,
                         "name": "string",
                         "softLimit": number
                      }
                   ],
                   "user": "string",
                   "vcpus": number,
                   "volumes": [
                      {
                         "host": {
                            "sourcePath": "string"
                         },
                         "name": "string"
                      }
                   ]
                }
        */
        if (node.has("command")) {
            JsonNode command = node.get("command");
            List<String> commandConstituents = new LinkedList();
            for (JsonNode n : command) {
                commandConstituents.add(n.asText());
            }
            builder.command(commandConstituents);
        }

        if (node.has("environment")) {
            JsonNode envPairs = node.get("environment");
            if (envPairs.isArray()) {
                for (JsonNode n : envPairs) {
                    builder.environment(
                            KeyValuePair.builder()
                                    .name(n.get("name").asText())
                                    .value(n.get("value").asText())
                                    .build());
                }
            } else {
                throw new RuntimeException("envPairs should be an array");
            }
        }
        if (node.has("image")) {
            builder.image(node.get("image").asText());
        }
        if (node.has("jobRoleArn")) {
            builder.jobRoleArn(node.get("jobRoleArn").asText());
        }
        if (node.has("memory")) {
            builder.memory(node.get("memory").asInt());
        }

        if (node.has("mountPoints")) {
            JsonNode mountPoints = node.get("mountPoints");
            // System.out.println( "MP " + mountPoints );
            if (mountPoints.isArray()) {
                for (JsonNode n : mountPoints) {
                    builder.mountPoints(
                            MountPoint.builder()
                                    .containerPath(n.get("containerPath").asText())
                                    .readOnly(n.get("readOnly").asBoolean())
                                    .sourceVolume(n.get("sourceVolume").asText())
                                    .build());
                }
            } else {
                throw new RuntimeException("mountPoints should be an array");
            }
        }

        if (node.has("privileged")) {
            builder.privileged(node.get("privileged").asBoolean());
        }
        if (node.has("readonlyRootFilesystem")) {
            builder.privileged(node.get("readonlyRootFilesystem").asBoolean());
        }
        if (node.has("ulimits")) {
            JsonNode ulimits = node.get("ulimits");
            if (ulimits.isArray()) {
                for (JsonNode n : ulimits) {
                    builder.ulimits(
                            Ulimit.builder()
                                    .hardLimit(n.get("hardLimit").asInt())
                                    .name(n.get("name").asText())
                                    .softLimit(n.get("softLimit").asInt())
                                    .build());
                }
            } else {
                throw new RuntimeException("ulimits should be an array");
            }
        }
        if (node.has("user")) {
            builder.user(node.get("user").asText());
        }
        if (node.has("vcpus")) {
            builder.vcpus(node.get("vcpus").asInt());
        }
        if (node.has("volumes")) {
            JsonNode volumes = node.get("volumes");
            if (volumes.isArray()) {
                for (JsonNode volume : volumes) {
                    Volume.Builder vBuilder = Volume.builder();
                    if (volume.has("host")) {
                        JsonNode host = volume.get("host");
                        if (host.has("sourcePath")) {
                            vBuilder.host(
                                    Host.builder()
                                            .sourcePath(host.get("sourcePath").asText())
                                            .build());
                        }
                    }
                    if (volume.has("name")) {
                        vBuilder.name(volume.get("name").asText());
                    }
                    builder.volumes(vBuilder.build());
                }
            } else {
                throw new RuntimeException("volumes should be an array");
            }
        }
        return builder.build();
    }

    private RegisterJobDefinitionRequest getTestRegisterJobDefinitionRequest(
            String accountID, String region, String basename, String suffix) {
        /*
        "RegisterJobDefinition": [
                {
                  "input": {
                    "type": "container",
                    "containerProperties": {
                      "command": [
                        "sleep",
                        "10"
                      ],
                      "image": "busybox",
                      "memory": 128,
                      "vcpus": 1
                    },
                    "jobDefinitionName": "sleep10"
                  },
                  "output": {
                    "jobDefinitionArn": "arn:aws:batch:us-east-1:012345678910:job-definition/sleep10:1",
                    "jobDefinitionName": "sleep10",
                    "revision": 1
                  },
                  "comments": {
                    "input": {
                    },
                    "output": {
                    }
                  },
                  "description": "This example registers a job definition for a simple container job.",
                  "id": "to-register-a-job-definition-1481154325325",
                  "title": "To register a job definition"
                }
              ],
        */
        Map<String, String> paramenters = new HashMap();
        RegisterJobDefinitionRequest.Builder builder = RegisterJobDefinitionRequest.builder();
        builder.jobDefinitionName(basename + suffix);
        builder.type(JobDefinitionType.Container);

        // build container properties required
        ContainerProperties.Builder containerBuilder = ContainerProperties.builder();
        containerBuilder.memory(500); // in mb
        containerBuilder.vcpus(1);
        containerBuilder.jobRoleArn("arn:aws:iam::" + accountID + ":role/aws-batch-basic");
        containerBuilder.image(
                accountID + ".dkr.ecr." + region + ".amazonaws.com/awsbatch/fetch_and_run");
        containerBuilder.environment(
                KeyValuePair.builder().name("PEGASUS_HOME").value("/usr").build());
        containerBuilder.user("nobody");

        builder.containerProperties(containerBuilder.build());

        return builder.build();
    }

    public static void main(String[] args) {
        JobDefinition jd = new JobDefinition();

        System.out.println(
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(
                        new File("/Users/vahi/NetBeansProjects/AWS-Batch/job-definition-http.json"),
                        "test"));
    }
}
