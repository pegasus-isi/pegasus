/*
 * 
 *   Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.util.VariableExpander;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A Reader class that uses BufferedReader to read in from a stream, and
 * perform variable expansion before returning as part of the read methods
 * 
 * @author Karan Vahi
 */
public class VariableExpansionReader extends Reader {

        private final BufferedReader mReader;
        
        /**
         * the buffer where we read in a line
         */
        private String mBuffer;
        
        private int mPosition;
        
        private final VariableExpander mVariableExpander;
        
        /**
         * 
     * @param reader
     * @throws java.io.IOException
         */
        public VariableExpansionReader( Reader reader) throws IOException {
            mReader = new BufferedReader( reader );
            mVariableExpander = new VariableExpander();
            setBufferToNextLine();
        }
        
        /**
         * Reads characters into a portion of an array. The characters read are
         * already susbstituted with their variable values.
         *
         * This method implements the general contract of the corresponding read
         * method of the BufferedReader class. As an additional convenience, it
         * attempts to read as many characters as possible by repeatedly
         * invoking the readLine method of the underlying BufferedReader and
         * doing the variable substitution. This iterated read continues until
         * one of the following conditions becomes true:
         *
         * The specified number of characters have been read, The read method of
         * the underlying reader returns -1, indicating end-of-file.
         *
         * If the first read on the underlying stream returns -1 to indicate
         * end-of-file then this method returns -1. Otherwise this method
         * returns the number of characters actually read.
         *
         * @param cbuf Destination buffer
         * @param off Offset at which to start storing characters
         * @param len Maximum number of characters to read
         *
         * @return The number of characters read, or -1 if the end of the stream
         * has been reached.
         * @throws IOException
         */
        public int read(char[] cbuf, int off, int len) throws IOException {
            
            int read = 0;
            
            if( mBuffer == null ){
                //end of stream reached
                return -1;
            }
            
            int store = off;
            while( true ){
                int bufferLength = mBuffer.length();
                //read characters from the internal buffer to destination buffer
                while( read < len && ( mPosition < bufferLength ) ){
                    cbuf[store++] = mBuffer.charAt(mPosition++);
                    read++;
                }
                if( mPosition == bufferLength ){
                    //we have exhausted our current buffer
                    setBufferToNextLine();
                }
                
                if( read == len || mBuffer == null ){
                    //break out of outerwhile
                    //we have the number of characters read
                    break;
                }
            }
            
            /*store = off;
            for( int i = 0; i < read; i++ ){
                System.out.print( cbuf[store++] );
            }*/
            return read;
        }
        
       

        @Override
        public void close() throws IOException {
            mReader.close();
        }

        private void setBufferToNextLine() throws IOException  {
            mPosition = 0;
            mBuffer  = mReader.readLine();
            //System.out.println("Buffer " + mBuffer );
            
            //mCurrentLineNumber = mReader.getLineNumber();
            //we don't want expand anything in the comment string
            //if( mBuffer != null && !mBuffer.startsWith( mCommentPrefix ) ){
            mBuffer = mVariableExpander.expand(mBuffer);
            
            //System.out.println( mBuffer );
            //}
            
            //always add \n to ensure consistent semantics for read function
            //w.r.t Reader class
            if( mBuffer != null ){
                mBuffer = mBuffer + '\n';
            }
        }
        
        
        public void reset() throws IOException{
            mReader.reset();
            mPosition = 0;
            mBuffer = "";
        }
        
    }