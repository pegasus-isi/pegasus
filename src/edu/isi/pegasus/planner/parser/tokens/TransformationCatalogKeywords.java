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
package edu.isi.pegasus.planner.parser.tokens;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mukund Murrali
 *
 */
public enum TransformationCatalogKeywords {

    NAMESPACE("namespace"),
    VERSION("version"),
    PROFILES("profiles"),
    METADATA("metadata"),
    SITES("sites"),
    SITE_PFN("pfn"),
    SITE_ARCHITECTURE("arch"),
    SITE_OS("os"),
    SITE_OS_TYPE("os.type"),
    SITE_OS_RELEASE("os.release"),
    SITE_OS_VERSION("os.version"),
    TYPE("type"),
    SITE_CONTAINER_NAME("container"),
    NAME("name"),
    CONTAINERS("containers"),
    CONTAINER_IMAGE("image"),
    CONTAINER_IMAGE_SITE("image_site"),
    CONTAINER_MOUNT("mount"),
    CONTAINER_DOCKERFILE("dockerfile"),
    REQUIRES("requires"),
    TRANSFORMATIONS("transformations");

    String name;

    static Map<String, TransformationCatalogKeywords> keywordsVsType = new HashMap<>();

    static {
        for (TransformationCatalogKeywords key : TransformationCatalogKeywords.values()) {
            keywordsVsType.put(key.getReservedName(), key);
        }
    }

    TransformationCatalogKeywords(String name) {
        this.name = name;
    }

    public String getReservedName() {
        return name;
    }

    public static TransformationCatalogKeywords getReservedKey(String key) {
        return keywordsVsType.get(key);
    }
}
