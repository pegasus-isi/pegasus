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
 * The Notification invoke object for the Dax API
 *
 * @author gmehta
 * @version $Revision$
 */
@JsonSerialize(using = Invoke.JsonSerializer.class)
public class Invoke {

    /** WHEN To INVOKE */
    public static enum WHEN {
        never,
        start,
        on_success,
        success,
        on_error,
        error,
        at_end,
        end,
        all
    };

    /** WHen to Invoke */
    protected WHEN mWhen;
    /** What to invoke */
    protected String mWhat;

    /**
     * Copy Constructor
     *
     * @param i the invoke object
     */
    public Invoke(Invoke i) {
        this(WHEN.valueOf(i.getWhen()), i.getWhat());
    }
    /**
     * Crete a new Invoke object
     *
     * @param when when to invoke
     */
    public Invoke(WHEN when) {
        setWhen(when);
    }

    /**
     * Create a new Invoke object
     *
     * @param when  when to invoke
     * @param what  the executable with the arguments to invoke
     */
    public Invoke(WHEN when, String what) {
        setWhen(when);
        mWhat = what;
    }

    /**
     * Get when to Invoke
     *
     * @return when the notification occurs
     */
    public String getWhen() {
        return mWhen.toString();
    }

    /**
     * Set when to invoke
     *
     * @param when when the notification occurs
     * @return Invoke
     */
    public final Invoke setWhen(WHEN when) {
        if (when.equals(WHEN.at_end)) {
            this.mWhen = WHEN.end;
        } else if (when.equals(WHEN.on_error)) {
            this.mWhen = WHEN.error;
        } else if (when.equals(WHEN.on_success)) {
            this.mWhen = WHEN.success;
        } else {
            this.mWhen = when;
        }
        return this;
    }

    /**
     * Get what to invoke
     *
     * @return the executable with arguments that are invoked
     */
    public String getWhat() {
        return mWhat;
    }

    /**
     * Set what executable to invoke and how
     *
     * @param what executable with the arguments
     * @return the Invoke
     */
    public Invoke setWhat(String what) {
        mWhat = what;
        return this;
    }

    /**
     * Create a copy of this Invoke object
     *
     * @return Invoke
     */
    public Invoke clone() {
        return new Invoke(this.mWhen, this.mWhat);
    }

    /**
     * Writes out XML representation using the writer
     * 
     * @param writer  the writer 
     * 
     */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    
    /**
     * Writes out XML representation using the writer
     * 
     * @param writer  the writer 
     * @param indent  number pf indent spaces
     */
    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("invoke", indent);
        writer.writeAttribute("when", mWhen.toString().toLowerCase());
        writer.writeData(mWhat).endElement();
    }

    /**
     * Returns the object as String
     *
     * @return the description
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[invoke ")
                .append("when=\"")
                .append(mWhen.toString().toLowerCase())
                .append("\"")
                .append(" what=\"")
                .append(mWhat)
                .append("\"]");
        return sb.toString();
    }

    /**
     * Matches two Invoke objects
     *
     * @param obj  object to being compared against
     * 
     * @return true if the pfn and all the attributes match, false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        // null check
        if (obj == null) {
            return false;
        }

        // see if type of objects match
        if (!(obj instanceof Invoke)) {
            return false;
        }

        Invoke i = (Invoke) obj;
        return this.toString().equals(i.toString());
    }

    /**
     * Custom serializer for YAML representation of an Invoke (Hook in 5.0)
     *
     * @author Ryan Tanaka
     */
    public static class JsonSerializer extends PegasusJsonSerializer<Invoke> {

        public JsonSerializer() {}

        /**
         * Serializes an Invoke into YAML representation.
         *
         * @param iv   the invoke object
         * @param gen  the json generator
         * @param sp   serialization provider
         * @throws IOException IOException
         */
        public void serialize(Invoke iv, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("_on", iv.getWhen());
            gen.writeStringField("cmd", iv.getWhat());
            gen.writeEndObject();
        }
    }
}
