/**
 * Copyright 2007-2010 University Of Southern California
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
package edu.isi.pegasus.common.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.StringCharacterIterator;

/**
 * This abstract class defines a common base for certain classes that deal with the generation of
 * XML files. Historically, this class also dealt with text generation, but those methods have been
 * mostly removed.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public abstract class XMLOutput {
    /**
     * Escapes certain characters inappropriate for textual output.
     *
     * <p>Since this method does not hurt, and may be useful in other regards, it will be retained
     * for now.
     *
     * @param original is a string that needs to be quoted
     * @return a string that is "safe" to print.
     */
    public static String escape(String original) {
        if (original == null) return null;
        StringBuilder result = new StringBuilder(2 * original.length());
        StringCharacterIterator i = new StringCharacterIterator(original);
        for (char ch = i.first(); ch != StringCharacterIterator.DONE; ch = i.next()) {
            if (ch == '\r') {
                result.append("\\r");
            } else if (ch == '\n') {
                result.append("\\n");
            } else if (ch == '\t') {
                result.append("\\t");
            } else {
                // DO NOT escape apostrophe. If apostrophe escaping is required,
                // do it beforehand.
                if (ch == '\"' || ch == '\\') result.append('\\');
                result.append(ch);
            }
        }

        return result.toString();
    }

    /**
     * Escapes certain characters inappropriate for XML content output.
     *
     * <p>According to the <a href="http://www.w3.org/TR/2008/REC-xml-20081126/#NT-AttValue">XML 1.0
     * Specification</a>, an attribute cannot contain ampersand, percent, nor the character that was
     * used to quote the attribute value.
     *
     * @param original is a string that needs to be quoted
     * @param isAttribute denotes an attributes value, if set to true. If false, it denotes regular
     *     XML content outside of attributes.
     * @return a string that is "safe" to print as XML.
     */
    public static String quote(String original, boolean isAttribute) {
        if (original == null) return null;
        StringBuilder result = new StringBuilder(2 * original.length());
        StringCharacterIterator i = new StringCharacterIterator(original);
        for (char ch = i.first(); ch != StringCharacterIterator.DONE; ch = i.next()) {
            switch (ch) {
                case '<':
                    if (isAttribute) result.append("&#60;");
                    else result.append("&lt;");
                    break;
                case '&':
                    if (isAttribute) result.append("&#38;");
                    else result.append("&amp;");
                    break;
                case '>':
                    // greater-than does not need to be attribute-escaped,
                    // but standard does not say we must not do it, either.
                    if (isAttribute) result.append("&#62;");
                    else result.append("&gt;");
                    break;
                case '\'':
                    if (isAttribute) result.append("&#39;");
                    else result.append("&apos;");
                    break;
                case '\"':
                    if (isAttribute) result.append("&#34;");
                    else result.append("&quot;");
                    break;
                default:
                    result.append(ch);
                    break;
            }
        }

        return result.toString();
    }

    /**
     * XML write helper method writes a quoted attribute onto a stream. The terminating quote will
     * be appended automatically. Values will be XML-escaped. No action will be taken, if the value
     * is null.
     *
     * @param stream is the stream to append to
     * @param key is the attribute including initial space, attribute name, equals sign, and opening
     *     quote. The string passed as key must never be <code>null</code>.
     * @param value is a string value, which will be put within the quotes and which will be
     *     escaped. If the value is null, no action will be taken
     * @throws IOException for stream errors.
     */
    public void writeAttribute(Writer stream, String key, String value) throws IOException {
        if (value != null) {
            stream.write(key);
            stream.write(quote(value, true));
            stream.write('"');
        }
    }

    /**
     * Saner XML write helper method writes a quoted attribute onto a stream. The value will be put
     * properly into quotes. Values will be XML-escaped. No action will be taken, if the key or
     * value are <code>null</code>.
     *
     * @param stream is the stream to append to
     * @param key is the attribute identifier, and just that.
     * @param value is a string value, which will be put within the quotes and which will be
     *     escaped. If the value is null, no action will be taken.
     * @throws IOException for stream errors.
     */
    public void writeAttribute2(Writer stream, String key, String value) throws IOException {
        if (key != null && value != null) {
            stream.write(key);
            stream.write("=\"");
            stream.write(quote(value, true));
            stream.write('"');
        }
    }

    /**
     * Dumps the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output.
     *
     * <p>Sibling classes which represent small leaf objects, and can return the necessary data more
     * efficiently, are encouraged to overwrite this method.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If null, avoidable whitespaces in the output will be avoided.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     * @throws IOException when encountering an error constructing the string.
     */
    public String toXML(String indent, String namespace) throws IOException {
        StringWriter sw = new StringWriter();
        this.toXML(sw, indent, namespace);
        sw.flush();
        return sw.toString();
    }

    /**
     * Convenience adaptor method invoking the equivalent of:
     *
     * <pre>
     * toXML( stream, indent, (String) null );
     * </pre>
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @throws IOException if something fishy happens to the stream.
     * @see #toXML( Writer, String, String )
     */
    public void toXML(Writer stream, String indent) throws IOException {
        toXML(stream, indent, (String) null);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you use a <b>buffered</b>
     * writer.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace. Use
     *     <code>null</code>, if you do not need an XML namespace.
     * @throws IOException if something fishy happens to the stream.
     * @see java.io.BufferedWriter
     */
    public abstract void toXML(Writer stream, String indent, String namespace) throws IOException;
}
