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

import edu.isi.pegasus.common.util.VariableExpander;
import edu.isi.pegasus.planner.parser.tokens.CloseBrace;
import edu.isi.pegasus.planner.parser.tokens.Identifier;
import edu.isi.pegasus.planner.parser.tokens.OpenBrace;
import edu.isi.pegasus.planner.parser.tokens.QuotedString;
import edu.isi.pegasus.planner.parser.tokens.Token;
import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogReservedWord;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;

/**
 * Implements the scanner for reserved words and other tokens that are generated from the input
 * stream for the Transformation Catalog.
 *
 * @author Jens Vöckler
 * @author Karan Vahi
 */
public class TransformationCatalogTextScanner {

    /** Stores the stream from which we are currently scanning. */
    private ExpanderLineNumberReader mInputReader;

    /** Captures the look-ahead character. */
    private int mLookAhead;

    /** Captures the previous token. Required to parse transformation value correctly. */
    private Token mPreviousToken;

    /**
     * Starts to scan the given stream.
     *
     * @param reader the reader stream from which we are reading the site catalog.
     * @param doVariableExpansion whether to expand variables or not
     */
    public TransformationCatalogTextScanner(Reader reader, boolean doVariableExpansion)
            throws IOException {
        this.mInputReader = new ExpanderLineNumberReader(reader, "#", doVariableExpansion);
        this.mLookAhead = mInputReader.read();
        // skipWhitespace();
    }

    /**
     * Obtains the current line number in the input stream from the outside.
     *
     * @return the current line number.
     */
    public int getLineNumber() {
        return mInputReader.getLineNumber();
    }

    /**
     * Skips any white space and comments in the input. This method stops either at the end of file,
     * or at any non-whitespace input character.
     */
    private void skipWhitespace() throws IOException {
        // end of file?
        if (mLookAhead == -1) {
            return;
        }

        // skip over whitespace
        while (mLookAhead != -1 && Character.isWhitespace((char) mLookAhead)) {
            mLookAhead = mInputReader.read();
        }

        // skip over comments until eoln
        if (mLookAhead == '#') {
            mInputReader.readLine();
            mLookAhead = mInputReader.read();
            skipWhitespace(); // FIXME: reformulate end-recursion into loop
        }
    }

    /**
     * Checks for the availability of more input.
     *
     * @return true, if there is more to read, false for EOF.
     */
    public boolean hasMoreTokens() throws IOException {
        skipWhitespace();
        return (this.mLookAhead != -1);
    }

    /**
     * Obtains the next token from the input stream.
     *
     * @return an instance conforming to the token interface, or null for eof.
     * @throws IOException if something went wrong while reading
     * @throws Exception if a lexical error was encountered.
     */
    public Token nextToken() throws IOException, ScannerException {
        // sanity check
        skipWhitespace();
        if (mLookAhead == -1) {
            mPreviousToken = null;
            return null;
        }

        // for identifier after tr we allow for . - : and / \
        boolean previousTokenIsTR = false;
        boolean previousTokenIsSiteOrCont = false;
        if ((mPreviousToken instanceof TransformationCatalogReservedWord
                && ((TransformationCatalogReservedWord) mPreviousToken).getValue()
                        == TransformationCatalogReservedWord.TRANSFORMATION)) {
            previousTokenIsTR = true;
        } else if (mPreviousToken instanceof TransformationCatalogReservedWord
                && (((TransformationCatalogReservedWord) mPreviousToken).getValue()
                                == TransformationCatalogReservedWord.SITE
                        || ((TransformationCatalogReservedWord) mPreviousToken).getValue()
                                == TransformationCatalogReservedWord.CONT)) {
            previousTokenIsSiteOrCont = true;
        }

        // are we parsing a reserved word or identifier
        if (Character.isJavaIdentifierStart((char) mLookAhead)) {
            StringBuffer identifier = new StringBuffer(8);
            identifier.append((char) mLookAhead);
            mLookAhead = mInputReader.read();

            if (previousTokenIsTR) {
                // allow : - / \ and . for transformation names
                while (mLookAhead != -1
                        && (Character.isJavaIdentifierPart((char) mLookAhead)
                                || mLookAhead == ':'
                                || mLookAhead == '.'
                                || mLookAhead == '-'
                                || mLookAhead == '/'
                                || mLookAhead == '\\'
                                || mLookAhead == '+') // PM-1377 allow + in tr names
                ) {
                    identifier.append((char) mLookAhead);
                    mLookAhead = mInputReader.read();
                }
            } else if (previousTokenIsSiteOrCont) {
                // allow - . @ in site names or container name
                while (mLookAhead != -1
                        && (Character.isJavaIdentifierPart((char) mLookAhead)
                                || mLookAhead == '-'
                                || mLookAhead == '.'
                                || mLookAhead == '@')) {
                    identifier.append((char) mLookAhead);
                    mLookAhead = mInputReader.read();
                }
            } else {
                // be more restrictive while parsing
                while (mLookAhead != -1 && Character.isJavaIdentifierPart((char) mLookAhead)) {
                    identifier.append((char) mLookAhead);
                    mLookAhead = mInputReader.read();
                }
            }

            // done parsing identifier or reserved word
            skipWhitespace();
            String s = identifier.toString().toLowerCase();
            if (TransformationCatalogReservedWord.symbolTable().containsKey(s)) {
                // isa reserved word
                mPreviousToken =
                        (TransformationCatalogReservedWord)
                                TransformationCatalogReservedWord.symbolTable().get(s);
            } else {
                // non-reserved identifier
                mPreviousToken = new Identifier(identifier.toString());
            }

        } else if (mLookAhead == '{') {
            mLookAhead = mInputReader.read();
            skipWhitespace();
            mPreviousToken = new OpenBrace();
        } else if (mLookAhead == '}') {
            mLookAhead = mInputReader.read();
            skipWhitespace();
            mPreviousToken = new CloseBrace();

        } else if (mLookAhead == '"') {
            // parser quoted string
            StringBuffer result = new StringBuffer(16);
            do {
                mLookAhead = mInputReader.read();
                if (mLookAhead == -1 || mLookAhead == '\r' || mLookAhead == '\n') {
                    // eof is an unterminated string
                    throw new ScannerException(
                            mInputReader.getLineNumber(), "unterminated quoted string");
                } else if (mLookAhead == '\\') {
                    int temp = mInputReader.read();
                    if (temp == -1) {
                        throw new ScannerException(
                                mInputReader.getLineNumber(),
                                "unterminated escape in quoted string");
                    } else {
                        // always add whatever is after the backslash
                        // FIXME: We could to fancy C-string style \012 \n \r escapes here ;-P
                        result.append((char) temp);
                    }
                } else if (mLookAhead != '"') {
                    result.append((char) mLookAhead);
                }
            } while (mLookAhead != '"');

            // skip over final quote
            mLookAhead = mInputReader.read();
            skipWhitespace();
            mPreviousToken = new QuotedString(result.toString());

        } else {
            // unknown material
            throw new ScannerException(
                    mInputReader.getLineNumber(), "unknown character " + ((char) mLookAhead));
        }

        return mPreviousToken;
    }

    /**
     * A wrapper around line reader, that allows us to do variable expansion, as and when each line
     * is read.
     */
    private static class ExpanderLineNumberReader {

        private LineNumberReader mReader;

        /** Handle to pegasus variable expander */
        private VariableExpander mVariableExpander;

        /** A StringBuffer on which we apply the expansion */
        private String mBuffer;

        /** Current position in the buffer to be read */
        private int mPosition;

        /** The corresponding line number of the line stored in the buffer */
        private int mCurrentLineNumber;

        /** Character indicating start of comment line */
        private String mCommentPrefix;

        /** Boolean indicating whether to do variable expansion or not */
        private boolean mDoVariableExpansion;

        /** @param reader */
        public ExpanderLineNumberReader(
                Reader reader, String commentPrefix, boolean doVariableExpansion)
                throws IOException {
            mReader = new LineNumberReader(reader);
            mVariableExpander = new VariableExpander();
            mCommentPrefix = commentPrefix;
            mDoVariableExpansion = doVariableExpansion;
            setBufferToNextLine();
        }

        private int read() throws IOException {
            if (mBuffer == null) {
                return -1;
            }

            if (mPosition == mBuffer.length()) {
                setBufferToNextLine();
                return read();
            }
            return mBuffer.charAt(mPosition++);
        }

        private String readLine() throws IOException {
            if (mBuffer == null) {
                return null;
            }

            String result = null;
            if (mPosition < mBuffer.length()) {
                // return substring from mPosition onwards
                result = mBuffer.substring(mPosition);
                setBufferToNextLine();
            } else {
                setBufferToNextLine();
                result = mBuffer.substring(mPosition);
            }

            return result;
        }

        private int getLineNumber() {
            return mCurrentLineNumber;
        }

        private void setBufferToNextLine() throws IOException {
            mPosition = 0;
            mBuffer = mReader.readLine();
            // System.out.println("Buffer " + mBuffer );

            mCurrentLineNumber = mReader.getLineNumber();
            // we don't want expand anything in the comment string
            if (mBuffer != null && !mBuffer.startsWith(mCommentPrefix)) {
                mBuffer = mDoVariableExpansion ? mVariableExpander.expand(mBuffer) : mBuffer;
            }

            // always add \n to ensure consistent semantics for read function
            // w.r.t Reader class
            if (mBuffer != null) {
                mBuffer = mBuffer + '\n';
            }
        }
    }
}
