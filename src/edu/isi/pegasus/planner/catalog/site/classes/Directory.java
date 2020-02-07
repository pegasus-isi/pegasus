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
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Directory class used for Site Catalog Schema version 4 onwards. The type of directory is
 * determined based on type attribute rather than having separate classes for it.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

@JsonDeserialize(using = DirectorySerializer.class)
public class Directory extends DirectoryLayout {

    /** Enumerates the new directory types supported in this schema */
    public static enum TYPE {
        shared_scratch("shared-scratch"),
        shared_storage("shared-storage"),
        local_scratch("local-scratch"),
        local_storage("local-storage");

        public static TYPE value(String name) {
            return TYPE.valueOf(name.replaceAll("-", "_"));
        }

        private String mValue;

        /**
         * The constructor
         *
         * @param value the string value to return
         */
        TYPE(String value) {
            mValue = value;
        }

        /**
         * The value associated with the enum
         *
         * @return
         */
        private String getValue() {
            return this.mValue;
        }

        /**
         * Override of the toString method to return
         *
         * @return
         */
        public String toString() {
            return this.getValue();
        }
    }

    /** Default constructor */
    public Directory() {
        super();
    }

    /**
     * Convenience constructor for adapter class
     *
     * @param directory the directory layout object
     * @param type the type associated
     */
    public Directory(DirectoryLayout directory, TYPE type) {
        super(directory);
        this.setType(type);
    }

    /**
     * Accept method for the SiteData classes that accepts a visitor
     *
     * @param visitor the visitor to be used
     * @exception IOException if something fishy happens to the stream.
     */
    public void accept(SiteDataVisitor visitor) throws IOException {
        visitor.visit(this);

        // traverse through all the file servers
        // for( FileServer server : this.mFileServers ){
        //    server.accept(visitor);
        // }

        for (OPERATION op : FileServer.OPERATION.values()) {
            List<FileServer> servers = this.mFileServers.get(op);
            for (FileServer server : servers) {
                server.accept(visitor);
            }
        }

        // profiles are handled in the depart method
        visitor.depart(this);
    }

    /** The type of directory */
    private TYPE mType;

    /**
     * Set the type of directory
     *
     * @param type the type of directory
     */
    public void setType(String type) {
        mType = TYPE.value(type);
    }

    /**
     * Set the type of directory
     *
     * @param type the type of directory
     */
    public void setType(Directory.TYPE type) {
        mType = type;
    }

    /**
     * Set the type of directory
     *
     * @return the type of directory
     */
    public TYPE getType() {
        return mType;
    }

    /**
     * @param writer
     * @param indent
     * @throws IOException
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String newIndent = indent + "\t";

        // sanity check?
        if (this.isEmpty()) {
            return;
        }

        // write out the  xml element
        writer.write(indent);
        writer.write("<directory ");
        writeAttribute(writer, "type", this.getType().toString());
        writer.write(">");
        writer.write(newLine);

        // iterate through all the file servers
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = this.getFileServersIterator(op); it.hasNext(); ) {
                FileServer fs = it.next();
                fs.toXML(writer, newIndent);
            }
        }

        // write out the internal mount point
        this.getInternalMountPoint().toXML(writer, newIndent);

        writer.write(indent);
        writer.write("</directory>");
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Directory obj;
        obj = (Directory) super.clone();

        obj.setType(this.getType());

        return obj;
    }
    
    public static void main(String[] args){
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        /*SimpleModule module = new SimpleModule();
        module.addDeserializer(FileServer.class, new FileServerDeserializer());
        mapper.registerModule(module);
        */
        String test = 
                "  type: sharedScratch\n" +
                "  path: /tmp/workflows/scratch\n" +
                "  fileServers:\n" +
                "    - operation: all\n" +
                "      url: file:///tmp/workflows/scratch";
        try {
            Directory dir = mapper.readValue(test, Directory.class);
            System.out.println(dir);
        } catch (IOException ex) {
            Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

/**
 * Custom deserializer for YAML representation of Directory
 * 
 * @author vahi
 */
class DirectorySerializer extends JsonDeserializer<Directory> {

    /**
     * Deserializes a Directory YAML description of the type
     * <pre>
          type: sharedScratch
          path: /tmp/workflows/scratch
          fileServers:
            - operation: all
              url: file:///tmp/workflows/scratch
     * </pre>
     * @param parser
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException 
     */
    @Override
    public Directory deserialize(JsonParser parser, DeserializationContext dc) throws IOException, JsonProcessingException {
        ObjectCodec oc = parser.getCodec();
        JsonNode node = oc.readTree(parser);
        Directory directory = new Directory();
        
        JsonNode fileServersNodes = node.get("fileServers");
        if (fileServersNodes != null) {
            parser = fileServersNodes.traverse(oc);
            List<FileServer> servers = parser.readValueAs(LinkedList.class);
            for (FileServer fs : servers) {
                directory.addFileServer(fs);
            }
            System.out.println(servers);
        }

        return directory;
        
    }
}