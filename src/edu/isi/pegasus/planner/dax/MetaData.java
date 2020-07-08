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
package edu.isi.pegasus.planner.dax;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import java.io.IOException;

/**
 * Metadata object for the DAX API
 *
 * @author gmehta
 * @version $Revision$
 */
@JsonSerialize(using = MetaData.JsonSerializer.class)
public class MetaData {

    /** Metadata Key */
    protected String mKey;
    /** Metadata type */
    protected String mType;
    /** Metadata value */
    protected String mValue;

    /**
     * Copy constructor
     *
     * @param m
     */
    public MetaData(MetaData m) {
        // create a copy
        this(m.getKey(), m.getType(), m.getValue());
    }

    /**
     * Create a new Metadata object
     *
     * @param key
     * @param value
     */
    public MetaData(String key, String value) {
        this(null, key, value);
    }

    /**
     * Create a new Metadata object
     *
     * @param type
     * @param key
     * @param value
     */
    private MetaData(String type, String key, String value) {
        mType = type;
        mKey = key;
        mValue = value;
    }

    /**
     * Create a copy of this Metdata Object
     *
     * @return
     */
    public MetaData clone() {
        return new MetaData(this.mType, this.mKey, this.mValue);
    }

    /**
     * Set the value of the metadata
     *
     * @param value
     * @return
     */
    public MetaData setValue(String value) {
        mValue = value;
        return this;
    }

    /**
     * Get the key of this metadata object
     *
     * @return
     */
    public String getKey() {
        return mKey;
    }

    /**
     * Get the type of the metdata object
     *
     * @return
     */
    private String getType() {
        return mType;
    }

    /**
     * Get the value of the metdata object
     *
     * @return
     */
    public String getValue() {
        return mValue;
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("metadata", indent);
        writer.writeAttribute("key", mKey).writeData(mValue).endElement();
    }

    /**
     * Custom serializer for YAML representation of a MetaData k,v pair
     *
     * @author Ryan Tanaka
     */
    public static class JsonSerializer extends PegasusJsonSerializer<MetaData> {

        public JsonSerializer() {}

        /**
         * Serializes a MetaData object in to YAML representation
         *
         * @param md
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(MetaData md, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField(md.getKey(), md.getValue());
            gen.writeEndObject();
        }
    }
}
