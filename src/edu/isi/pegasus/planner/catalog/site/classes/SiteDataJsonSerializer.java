/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.catalog.site.classes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Collection;

/**
 * Base class from which all Site Data serializers extend
 *
 * @author Karan Vahi
 * @param <T>
 */
public abstract class SiteDataJsonSerializer<T> extends JsonSerializer<T> {

    

    /**
     * Writes out only if value is not null and non empty
     *
     * @param gen
     * @param key
     * @param value
     * @throws java.io.IOException
     */
    public void writeStringField(JsonGenerator gen, String key, String value) throws IOException{
        if (value != null && value.length() > 0) {
            gen.writeStringField(key, value);
        }
    }
    
    /**
     * Writes out only if value is not null and non empty
     *
     * @param gen
     * @param key
     * @param value
     * @throws java.io.IOException
     */
    public void writeArray(JsonGenerator gen, String key, Collection value) throws IOException{
        if (value != null && !value.isEmpty()) {
            gen.writeFieldName(key);
            gen.writeObject(value);
        }
    }


}

