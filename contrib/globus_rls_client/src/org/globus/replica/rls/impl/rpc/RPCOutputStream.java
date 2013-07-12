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

import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * An output stream filter for RLS RPC protocol messages. 
 *
 */
class RPCOutputStream extends FilterOutputStream {

	// CONSTANTS //
	
	/** Message terminator (as defined by the RLS RPC protocol). */
	private final static int TERMINATOR = 0;
	
	/** DEBUG flag */
	private final static boolean DEBUG = false;
	
	/** RLS Date Format */
	private final static String RLS_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
	
	// FIELDS //
	
	private SimpleDateFormat sdf = new SimpleDateFormat(RLS_DATE_FORMAT); 

	// CONSTRUCTORS //
	
	RPCOutputStream(OutputStream out) {
		super(out);
	}

	// METHODS //
	
	/**
	 * Write RPC formatted string to output stream.
	 * @param str Java String to write
	 * @throws IOException
	 */
	public final void writeString(String str) throws IOException {
		// RLS RPC sends 0 bytes + TERMINATOR for null strings
		if (str == null)
			str = "";
		
		_debug(str);
		write(str.getBytes(), 0, str.length());
		writeTerminator();
	}
	
	/**
	 * Write RPC terminator. This terminator is often used to terminate input
	 * parameter lists and individual strings.
	 * @throws IOException
	 */
	public final void writeTerminator() throws IOException {
		_debug("TERMINATOR");
		write(TERMINATOR);
	}

	/**
	 * Write RPC formatted int to output stream.
	 * @param i Java int type value to write
	 * @throws IOException
	 */
	public final void writeInt(int i) throws IOException {
		_debug("(int) " + i);
		writeString(Integer.toString(i));
	}
	
	/**
	 * Write RPC formatted double to output stream.
	 * @param d Java double type value to write
	 * @throws IOException
	 */
	public final void writeDouble(double d) throws IOException {
		_debug("(double) " + d);
		writeString(Double.toString(d));
	}
	
	/**
	 * Write RPC formatted double to output stream.
	 * @param d Java double type value to write
	 * @throws IOException
	 */
	public final void writeDate(Date d) throws IOException {
		_debug("(Date) " + d);
		writeString(sdf.format(d));
	}
	
	/** Debug output */
	private void _debug(String msg) {
		if (DEBUG)
			System.err.println("Write to stream: " + msg);
	}
}
