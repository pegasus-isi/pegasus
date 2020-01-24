/**
 * Copyright 2007-2008 University Of Southern California
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

import java.util.List;

/**
 * This is for storing the YAML Schema Result for a given YAML file.
 *
 * @author Mukund Murrali
 */
class YAMLSchemaValidationResult {

    /** This denotes if the yaml validation is successful or not */
    private boolean isSuccess;

    /** This holds the list of error messages for the yaml file */
    private List<String> errorMessage;

    void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    void setErrorMessage(List<String> errorMessage) {
        this.errorMessage = errorMessage;
    }

    boolean isSuccess() {
        return isSuccess;
    }

    List<String> getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "success = " + isSuccess + "\t error messages = " + errorMessage.toString();
    }
}
