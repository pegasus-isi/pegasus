/*
 * Copyright 1999-2006 University of Chicago
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.replica.rls.impl.rpc;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Vector;

/**
 * An input stream filter for RLS RPC protocol messages. 
 *
 */
class RPCInputStream extends FilterInputStream {

	// CONSTANTS //
	
	/** End of stream (as defined by {@link java.io.InputStream#read()}. */
	private final int EOS = -1;
	/** Message terminator (as defined by the RLS RPC protocol). */
	private final int TERMINATOR = 0;
	
	/** DEBUG flag */
	private final boolean DEBUG = false;
	
	/** RLS Date Format */
	private final static String RLS_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
	
	// FIELDS //
	
	private SimpleDateFormat sdf = new SimpleDateFormat(RLS_DATE_FORMAT); 

	// CONSTRUCTORS //
	
	RPCInputStream(InputStream in) {
		super(in);
	}

	// METHODS //
	
	/**
	 * Reads an RLS RPC integer from the input buffer.
	 * @return An int read from the input buffer.
	 * @throws IOException
	 */
	public final int readInt() throws IOException {
		return Integer.parseInt(readString());
	}
	
	/**
	 * Reads an RLS RPC integer from the input buffer.
	 * @return An Integer read from the input buffer or null if no integer
	 * 		available from the stream.
	 * @throws IOException
	 */
	public final Integer readInteger() throws IOException {
		String s = readString();
		if (s == null || s.length()==0)
			return null;
		return new Integer(s);
	}
	
	/**
	 * Reads an RLS RPC double from the input buffer.
	 * @return A double read from the input buffer.
	 * @throws IOException
	 */
	public final double readDouble() throws IOException {
		return Double.parseDouble(readString());
	}
	
	/**
	 * Reads an RLS RPC date from the input buffer.
	 * @return A date read from the input buffer.
	 * @throws IOException
	 */
	public final Date readDate() throws IOException {
		String raw = readString();
		try {
			return sdf.parse(raw);
		}
		catch(ParseException pe) {
			throw new IOException("Failed to parse date from stream (cause: " +
					pe.getMessage() + ")");
		}
	}

	/**
	 * Reads an RLS RPC string from the input buffer.
	 * @return A string read from the input buffer.
	 * @throws IOException
	 */
	public final String readString() throws IOException {
		StringBuffer buffer = new StringBuffer();
		for (int b = read(); b>TERMINATOR; b = read())
			buffer.append((char)b);
		_debug(buffer.toString());
		if (buffer.length() == 0)
			return null;
		else
			return buffer.toString();
	}
	
	/** Debug input */
	private void _debug(String msg) {
		if (DEBUG)
			System.err.println("Read from stream: " + msg);
	}
}