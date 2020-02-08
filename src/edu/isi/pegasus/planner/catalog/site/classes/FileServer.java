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

package edu.isi.pegasus.planner.catalog.site.classes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.CatalogException;

import edu.isi.pegasus.planner.catalog.classes.Profiles;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class describes a file server that can be used to stage data to and from a site.
 *
 * @author Karan Vahi
 */
@JsonDeserialize(using = FileServerDeserializer.class)
public class FileServer extends FileServerType {

    /** The default constructor. */
    public FileServer() {
        super();
    }

    /**
     * Overloaded constructor.
     *
     * @param protocol protocol employed by the File Server.
     * @param urlPrefix the url prefix
     * @param mountPoint the mount point for the server.
     */
    public FileServer(String protocol, String urlPrefix, String mountPoint) {
        super(protocol, urlPrefix, mountPoint);
    }

    /**
     * Returns the externally accessible URL composed of url prefix and the mount point
     *
     * @return
     */
    public String getURL() {
        return this.getURLPrefix() + this.getMountPoint();
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String newIndent = indent + "\t";

        // write out the  xml element
        writer.write(indent);
        writer.write("<file-server");

        writeAttribute(writer, "protocol", getProtocol());
        writeAttribute(writer, "url", getURLPrefix());
        writeAttribute(writer, "mount-point", getMountPoint());
        writeAttribute(writer, "operation", getSupportedOperation().toString());

        if (mProfiles.isEmpty()) {
            writer.write("/>");
        } else {
            writer.write(">");
            writer.write(newLine);

            mProfiles.toXML(writer, newIndent);

            writer.write(indent);
            writer.write("</file-server>");
        }
        writer.write(newLine);
    }

    /** @param visitor */
    public void accept(SiteDataVisitor visitor) throws IOException {
        visitor.visit(this);

        visitor.depart(this);
    }

    /**
     * Returns the associated profiles
     *
     * @return
     */
    public Profiles getProfiles() {
        return this.mProfiles;
    }

    public static void main(String[] args){
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        /*SimpleModule module = new SimpleModule();
        module.addDeserializer(FileServer.class, new FileServerDeserializer());
        mapper.registerModule(module);
        */
        String test = 
                "operation: all\n" +
                "url: file:///tmp/workflows/scratch";
        try {
            FileServer fs = mapper.readValue(test, FileServer.class);
            System.out.println(fs);
        } catch (IOException ex) {
            Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

/**
 * Custom deserializer for YAML representation of FileServer
 * 
 * @author vahi
 */
class FileServerDeserializer extends SiteDataJsonDeserializer<FileServer> {

    /**
     * Deserializes a FileServer YAML description of the type
     * <pre>
         - operation: all
           url: file:///tmp/workflows/scratch
     * </pre>
     * @param jp
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException 
     */
    @Override
    public FileServer deserialize(JsonParser jp, DeserializationContext dc) throws IOException, JsonProcessingException {
        ObjectCodec oc = jp.getCodec();
        JsonNode node = oc.readTree(jp);
        FileServer fs = new FileServer();
        
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            SiteCatalogKeywords reservedKey =
                    SiteCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(SiteCatalogKeywords.FILESERVERS.getReservedName(), key, node );
            }

            switch (reservedKey) {
                case OPERATION:
                    fs.setSupportedOperation(node.get(key).asText());
                    break;
                    
                case URL:
                    String fullURL = node.get(key).asText();
                    PegasusURL url = new PegasusURL(fullURL);
                    fs.setURLPrefix(url.getURLPrefix());
                    fs.setProtocol(url.getProtocol());
                    fs.setMountPoint(url.getPath());
                    break;
                    
                default:
                    this.complainForUnsupportedKey(SiteCatalogKeywords.FILESERVERS.getReservedName(), key, node);
            }
        }
        
        return fs;
    }
}


