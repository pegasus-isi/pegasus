/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package edu.isi.pegasus.planner.parser;

import org.griphyn.vdl.parser.*;
import edu.isi.pegasus.planner.invocation.CPU;
import edu.isi.pegasus.planner.invocation.HasText;
import edu.isi.pegasus.planner.invocation.JobStatus;
import edu.isi.pegasus.planner.invocation.StatCall;
import edu.isi.pegasus.planner.invocation.Architecture;
import edu.isi.pegasus.planner.invocation.Machine;
import edu.isi.pegasus.planner.invocation.StatInfo;
import edu.isi.pegasus.planner.invocation.JobStatusSignal;
import edu.isi.pegasus.planner.invocation.Regular;
import edu.isi.pegasus.planner.invocation.ArgEntry;
import edu.isi.pegasus.planner.invocation.ArgVector;
import edu.isi.pegasus.planner.invocation.Proc;
import edu.isi.pegasus.planner.invocation.Fifo;
import edu.isi.pegasus.planner.invocation.Temporary;
import edu.isi.pegasus.planner.invocation.EnvEntry;
import edu.isi.pegasus.planner.invocation.MachineSpecific;
import edu.isi.pegasus.planner.invocation.JobStatusSuspend;
import edu.isi.pegasus.planner.invocation.Descriptor;
import edu.isi.pegasus.planner.invocation.Task;
import edu.isi.pegasus.planner.invocation.Load;
import edu.isi.pegasus.planner.invocation.Environment;
import edu.isi.pegasus.planner.invocation.Usage;
import edu.isi.pegasus.planner.invocation.Boot;
import edu.isi.pegasus.planner.invocation.MachineInfo;
import edu.isi.pegasus.planner.invocation.Invocation;
import edu.isi.pegasus.planner.invocation.InvocationRecord;
import edu.isi.pegasus.planner.invocation.RAM;
import edu.isi.pegasus.planner.invocation.WorkingDir;
import edu.isi.pegasus.planner.invocation.Arguments;
import edu.isi.pegasus.planner.invocation.JobStatusRegular;
import edu.isi.pegasus.planner.invocation.ArgString;
import edu.isi.pegasus.planner.invocation.Data;
import edu.isi.pegasus.planner.invocation.Stamp;
import edu.isi.pegasus.planner.invocation.Uname;
import edu.isi.pegasus.planner.invocation.Swap;
import edu.isi.pegasus.planner.invocation.Job;
import edu.isi.pegasus.planner.invocation.Status;
import edu.isi.pegasus.planner.invocation.Ignore;
import edu.isi.pegasus.planner.invocation.JobStatusFailure;

import org.griphyn.vdl.util.Logging;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import java.io.*;
import java.util.*;
import java.text.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML
 * document which contains information from kickstart generated
 * invocation record.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 *
 */
public class InvocationParser extends DefaultHandler
{
  /**
   * Default parser is the Xerces parser.
   */
  protected static final String vendorParserClass =
    "org.apache.xerces.parsers.SAXParser";

  /**
   * Holds the instance of a {@link org.xml.sax.XMLReader} class.
   */
  private XMLReader m_parser;

  /**
   * Holds the result, will be overwritten by each invocation of parse().
   */
  private InvocationRecord m_result;

  /**
   * Keep the location within the document
   */
  private Locator m_location;

  /**
   * A Hashmap to forward resolve namespaces that were encountered
   * during parsing.
   */
  private Map m_forward;

  /**
   * A Hashmap to reverse resolve namespaces that were encountered
   * during parsing.
   */
  private Map m_reverse;

  /**
   * Parsing for ISO dates without milliseconds
   */
  private SimpleDateFormat m_coarse;

  /**
   * Parsing for ISO dates with millisecond extension.
   */
  private SimpleDateFormat m_fine;

  /**
   * Obtain our logger once for multiple uses.
   */
  private Logging m_log;

  /**
   * Count the depths of elements in the document
   */
  private int m_depth = 0;

  /**
   * A stack of namespaces?
   */
  private Stack m_stack;

  /**
   * Sets a feature while capturing failed features right here.
   *
   * @param uri is the feature's URI to modify
   * @param flag is the new value to set.
   * @return true, if the feature could be set, false for an exception
   */
  private boolean set( String uri, boolean flag )
  {
    boolean result = false;
    try {
      this.m_parser.setFeature( uri, flag );
      result = true;
    } catch ( SAXException se ) {
      Logging.instance().log( "default", 0,
			      "Could not set parser feature " +
			      se.getMessage() );
    }
    return result;
  }

  /**
   * The class constructor. This function initializes the Xerces parser
   * and the features that enable schema validation.
   *
   * @param schemaLocation is the default location of the XML Schema
   * which this parser is capable of parsing. It may be null to use
   * the defaults provided in the document.
   */
  public InvocationParser( String schemaLocation )
  {
    this.m_forward = new HashMap();
    this.m_reverse = new HashMap();
    this.m_coarse = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ssZ" );
    this.m_fine = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSZ" );
    this.m_log = Logging.instance();

    try {
      m_parser = (XMLReader) Class.forName(vendorParserClass).newInstance();
      m_parser.setContentHandler(this);
      // m_parser.setErrorHandler(this);
      m_parser.setErrorHandler( new VDLErrorHandler() );

      set( "http://xml.org/sax/features/validation", true );
      set( "http://apache.org/xml/features/validation/dynamic", true );
      set( "http://apache.org/xml/features/validation/schema", true );
      // time+memory consuming, see http://xml.apache.org/xerces2-j/features.html
      // set( "http://apache.org/xml/features/validation/schema-full-checking", true );

      // Send XML Schema element default values via characters().
      set( "http://apache.org/xml/features/validation/schema/element-default", true );
      set( "http://apache.org/xml/features/validation/warn-on-duplicate-attdef", true );
      // mysteriously, this one fails with recent Xerces
      // set( "http://apache.org/xml/features/validation/warn-on-undeclared-elemdef", true );
      set( "http://apache.org/xml/features/warn-on-duplicate-entitydef", true );
      set( "http://apache.org/xml/features/honour-all-schemaLocations", true );

      // set the schema default location.
      if ( schemaLocation != null ) {
	setSchemaLocations( InvocationRecord.SCHEMA_NAMESPACE + ' ' +
			    schemaLocation );
	m_log.log("app", 2, "will use " + schemaLocation );
      } else {
	m_log.log("app", 2, "will use document schema hint" );
      }
    } catch (ClassNotFoundException e) {
      m_log.log( "defaut", 0,
		 "The SAXParser class was not found: " + e);
    } catch (InstantiationException e) {
      m_log.log( "default", 0,
		 "The SAXParser class could not be instantiated: " + e);
    } catch (IllegalAccessException e) {
      m_log.log( "default", 0,
		 "The SAXParser class could not be accessed: " + e);
    }
  }

  /**
   * Sets the list of external real locations where the XML schema may
   * be found. Since this list can be determined at run-time through
   * properties etc., we expect this function to be called between
   * instantiating the parser, and using the parser
   *
   * @param list is a list of strings representing schema locations. The
   * content exists in pairs, one of the namespace URI, one of the
   * location URL.
   */
  public void setSchemaLocations( String list )
  {
    // schema location handling
    try {
      m_parser.setProperty(
	"http://apache.org/xml/properties/schema/external-schemaLocation",
	list );
    } catch ( SAXException se ) {
      m_log.log( "default", 0,
		 "The SAXParser reported an error: " + se );
    }
  }

  /**
   * This function parses a XML source from an InputStream source, and
   * creates java class instances that correspond to different elements
   * in the XML source.
   *
   * @param reader is a bytestream opened for reading.
   * @return the records with the invocation information, or null on failure.
   */
  public InvocationRecord parse( java.io.InputStream reader )
  {
    try {
      // will change m_result
      m_parser.parse( new InputSource(reader) );
      return m_result;
    } catch (SAXException e) {
      // e.printStackTrace( System.err );
      m_log.log( "default", 0, "SAX Error: " + e.getMessage() );
    } catch (IOException e) {
      m_log.log( "default", 0, "IO Error: " + e.getMessage() );
    }

    return null;
  }

  /**
   * This function parses a XML source from the new Reader source, and
   * creates java class instances that correspond to different elements
   * in the XML source.
   *
   * @param reader is a character stream opened for reading.
   * @return the records with the invocation information, or null on failure.
   */
  public InvocationRecord parse( java.io.Reader reader )
  {
    try {
      // will change m_result
      m_parser.parse( new InputSource(reader) );
      return m_result;
    } catch (SAXException e) {
      // e.printStackTrace( System.err );
      m_log.log( "default", 0, "SAX Error: " + e.getMessage() );
    } catch (IOException e) {
      m_log.log( "default", 0, "IO Error: " + e.getMessage() );
    }

    return null;
  }

  //
  // here starts the implementation to the Interface
  //

  /**
   * Obtains the document locator from the parser. The document location
   * can be used to print debug information, i.e the current location
   * (line, column) in the document.
   *
   * @param locator is the externally set current position
   */
  public void setDocumentLocator( Locator locator )
  {
    this.m_location = locator;
  }

  private String full_where()
  {
    return ( "line " + m_location.getLineNumber() +
	     ", col " + m_location.getColumnNumber() );
  }

  private String where()
  {
    return ( m_location.getLineNumber() +
	     ":" +
	     m_location.getColumnNumber() );
  }

  /**
   * This method specifies what to do when the parser is at the beginning
   * of the document. In this case, we simply print a message for debugging.
   */
  public void startDocument()
  {
    this.m_depth = 0;
    this.m_stack = new Stack();
    this.m_log.log( "parser", 1, "*** start of document ***" );
  }

  /**
   * The parser comes to the end of the document.
   */
  public void endDocument()
  {
    this.m_log.log( "parser", 1, "*** end of document ***" );
  }

  /**
   * There is a prefix or namespace defined, put the prefix and its URI
   * in the HashMap. We can get the URI when the prefix is used here after.
   *
   * @param prefix the Namespace prefix being declared.
   * @param uri the Namespace URI the prefix is mapped to.
   */
  public void startPrefixMapping( java.lang.String prefix,
                                  java.lang.String uri )
    throws SAXException
  {
    String p = prefix == null ? null : new String(prefix);
    String u = uri == null ? null : new String(uri);
    m_log.log( "parser", 2, "adding \"" + p + "\" <=> " + u );

    if ( ! this.m_forward.containsKey(p) )
      this.m_forward.put(p, new Stack());
    ((Stack) this.m_forward.get(p)).push(u);

    if ( ! this.m_reverse.containsKey(u) )
      this.m_reverse.put(u, new Stack());
    ((Stack) this.m_reverse.get(u)).push(p);
  }


  /**
   * Out of the reach of the prefix, remove it from the HashMap.
   *
   * @param prefix is the prefix that was being mapped previously.
   */
  public void endPrefixMapping( java.lang.String prefix )
    throws SAXException
  {
    String u = (String) ((Stack) this.m_forward.get(prefix)).pop();
    String p = (String) ((Stack) this.m_reverse.get(u)).pop();
    m_log.log( "parser", 2, "removed \"" + p + "\" <=> " + u );
  }

  /**
   * Helper function to map prefixes correctly onto the elements.
   *
   * @param uri is the parser-returned URI that needs translation.
   * @return the correct prefix for the URI
   */
  private String map( String uri )
  {
    if ( uri == null || uri.length() == 0 ) return "";
    Stack stack = (Stack) this.m_reverse.get(uri);
    String result = stack == null ? null : (String) stack.peek();
    if ( result == null || result.length() == 0 ) return "";
    else return result + ':';
  }


  /**
   * This method defines the action to take when the parser begins to parse
   * an element.
   *
   * @param namespaceURI is the URI of the namespace for the element
   * @param localName is the element name without namespace
   * @param qName is the element name as it appears in the docment
   * @param atts has the names and values of all the attributes
   */
  public void startElement( java.lang.String namespaceURI,
                            java.lang.String localName,
                            java.lang.String qName,
                            Attributes atts )
    throws SAXException
  {
    m_log.log( "parser", 3,
	       "<" + map(namespaceURI) + localName + "> at " +
	       where() );

    // yup, one more element level
    m_depth++;

    java.util.List names = new java.util.ArrayList();
    java.util.List values = new java.util.ArrayList();
    for ( int i=0; i < atts.getLength(); ++i ) {
      String name = new String( atts.getLocalName(i) );
      String value = new String( atts.getValue(i) );

      m_log.log( "parser", 2, "attribute " + map(atts.getURI(i)) +
		 name + "=\"" + value + "\"" );
      names.add(name);
      values.add(value);
    }

    //System.out.println( "QNAME " + qName + " NAME " + names + "\t Values" + values );
    Invocation parent = null;
    if ( ! m_stack.empty() ) {
        IVSElement peek = (IVSElement) m_stack.peek();
        parent = (Invocation)peek.m_obj;
    }
    Invocation object = createObject( parent, qName, names, values );
    if ( object != null )
      m_stack.push( new IVSElement( qName, object ) );
    else
      throw new SAXException( "empty element while parsing" );
  }

  /**
   * The parser is at the end of an element. Each successfully and
   * completely parsed Definition will trigger a callback to the
   * registered DefinitionHandler.
   *
   * @param namespaceURI is the URI of the namespace for the element
   * @param localName is the element name without namespace
   * @param qName is the element name as it appears in the docment
   */
  public void endElement( java.lang.String namespaceURI,
                          java.lang.String localName,
                          java.lang.String qName )
    throws SAXException
  {
    // that's it for this level
    m_depth--;
    m_log.log( "parser", 3,
	       "</" + map(namespaceURI) + localName + "> at " +
	       where() );

    IVSElement tos = (IVSElement) m_stack.pop();
    if ( ! qName.equals(tos.m_name) ) {
      m_log.log( "default", 0, "assertion failure" );
      System.exit(1);
    }

    if ( ! m_stack.empty() ) {
      // add pieces to lower levels
      IVSElement peek = (IVSElement) m_stack.peek();
      if ( !setElementRelation( peek.m_name.charAt(0), peek.m_obj, tos.m_obj )){
	m_log.log( "parser", 0, "Element " + tos.m_name +
		   " does not fit into element " + peek.m_name );
        //System.out.println(  "Element " + tos.m_name +
	//	   " does not fit into element " + peek.m_name );
      }
    } else {
      // run finalizer, if available
      // m_log.log( "default", 0, "How did I get here?" );
    }
  }

  /**
   * This method is the callback function for characters in an element.
   * The element is expected to be of mixed content.
   *
   * @param ch are the characters from the XML document
   * @param start is the start position into the array
   * @param length is the amount of valid data in the array
   */
  public void characters( char[] ch, int start, int length )
    throws SAXException
  {
    String message = new String( ch, start, length );
    if ( message.length() > 0  ) {
      if ( message.trim().length() == 0 )
        m_log.log( "parser", 3, "Characters: \' \' x " +
                                message.length() );
      else
        m_log.log( "parser", 3, "Characters: \"" + message  + "\"" );

      // Insert text into the text carrying elements. These elements
      // must be capable to have text added repeatedly.
      if ( ! m_stack.empty() ) {
	IVSElement tos = (IVSElement) m_stack.peek();
	if ( tos.m_obj instanceof HasText ) {
	  HasText obj = (HasText) tos.m_obj;
	  obj.appendValue(message);
	}
      } else {
	// run finalizer, if available
	m_log.log( "default", 0, "How did I get here II?" );
      }
    }
  }

  /**
   * Currently, ignorable whitespace will be ignored.
   *
   * @param ch are the characters from the XML document
   * @param start is the start position into the array
   * @param length is the amount of valid data in the array
   */
  public void ignorableWhitespace( char[] ch, int start, int length )
    throws SAXException
  {
    // not implemented
  }

  /**
   * Receive a processing instruction. Currently, we are just printing
   * a debug message that we received a PI.
   *
   * @param target the processing instruction target
   * @param data the processing instruction data, or null if none was supplied.
   * The data does not include any whitespace separating it from the target.
   */
  public void processingInstruction( java.lang.String target,
                                     java.lang.String data )
    throws SAXException
  {
    m_log.log( "parser", 2, "processing instruction " + target +
	       "=\"" + data + "\" was skipped!");
  }

  /**
   * Receive a notification that an entity was skipped. Currently, we
   * are just printing a debug message to this fact.
   *
   * @param name The name of the skipped entity. If it is a parameter
   * entity, the name will begin with '%', and if it is the external DTD
   * subset, it will be the string "[dtd]".
   */
  public void skippedEntity(java.lang.String name)
    throws SAXException
  {
    m_log.log( "parser", 2,
	       "entity " + name + " was skipped!");
  }

  //
  // =================================================== our own stuff ===
  //


  /**
   * Small helper method to bundle repetitive parameters in a template
   * for reporting progress.
   *
   * @param subject is the name of the XML element that is being scrutinized.
   * @param name is then name of the element we are working with.
   * @param value is the attribute value.
   */
  private void log( String subject, String name, String value )
  {
    if ( value == null ) value = new String();
    m_log.log( "filler", 3, subject + "." + name + "=\"" +
	       value + "\"" );
  }

  /**
   * Small helper method to bundle repetitive complaints in a template
   * for reporting progress.
   *
   * @param subject is the name of the XML element that is being scrutinized.
   * @param name is then name of the element we are working with.
   * @param value is the attribute value.
   */
  private void complain( String subject, String name, String value )
  {
    if ( value == null ) value = new String();
    m_log.log( "default", 0, "ignoring " + subject + '@' + name +
	       "=\"" + value + '"', true );
  }

  /**
   * Small helper to parse the different date varieties and deal with
   * Java obnoxeity.
   *
   * @param date is an ISO 8601 timestamp
   * @return a date field
   * @exception ParseException thrown if the date cannot be parsed
   */
  private Date parseDate( String date )
    throws ParseException
  {
    // SimpleDataFormat stumbles over colon in time zone
    int size = date.length();
    if ( date.charAt(size-3) == ':' ) {
      StringBuffer temp = new StringBuffer(date);
      temp.deleteCharAt(size-3);
      date = temp.toString();
    }

    Date result;
    if ( date.indexOf('.') == -1 ) {
      // coarse grained timestamp
      result = m_coarse.parse(date);
    } else {
      // fine grained timestamp
      result = m_fine.parse(date);
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ");
    m_log.log( "filler", 3, "found date " + sdf.format(result) );
    return result;
  }

  /**
   * Small helper method to set up the attributes for the job elements.
   *
   * @param job is the job to set up.
   * @param names is the list of attribute names
   * @param values is the list of attribute values
   */
  private void setupJob( Job job, java.util.List names, java.util.List values )
    throws NumberFormatException, ParseException
  {
    for ( int i=0; i<names.size(); ++i ) {
      String name = (String) names.get(i);
      String value = (String) values.get(i);

      if ( name.equals("start") ) {
	this.log( job.getTag(), name, value );
	job.setStart( parseDate(value) );
      } else if ( name.equals("duration") ) {
	this.log( job.getTag(), name, value );
	job.setDuration( Double.parseDouble(value) );
      } else if ( name.equals("pid") ) {
	this.log( job.getTag(), name, value );
	job.setPID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
      } else {
	this.complain( job.getTag(), name, value );
      }
    }
  }

  /**
   * This method determines the actively parsed element, creates the
   * Java object that corresponds to the element, and sets the member
   * variables with the values of the attributes of the element.
   *
   * @param parent  is the parent element
   * @param e is the name of the element
   * @param names  is a list of attribute names, as strings.
   * @param values  is a list of attribute values, to match the key list.
   * @return A new VDL Java object, which may only be partly constructed.
   * @exception IllegalArgumentException if the element name is too short.
   */
  protected Invocation createObject( Invocation parent,
				     String e,
				     java.util.List names,
				     java.util.List values )
    throws IllegalArgumentException
  {
    if ( e == null || e.length() < 1 )
      throw new IllegalArgumentException("illegal element length");

    try {
      // postcondition: string has content w/ length > 0
      switch ( e.charAt(0) ) {
	//
	// A
	//
      case 'a':
	if ( e.equals("arg") ) {
	  ArgEntry entry = new ArgEntry();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("nr") ) {
	      this.log( e, name, value );
	      entry.setPosition( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return entry;

	} else if ( e.equals("arguments") ) {
	  Arguments cli = new ArgString();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("executable") ) {
	      this.log( e, name, value );
	      cli.setExecutable(value);
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return cli;

	} else if ( e.equals("argument-vector") ) {
	  Arguments cli = new ArgVector();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("executable") ) {
	      this.log( e, name, value );
	      cli.setExecutable(value);
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return cli;
	}

	// unknown
	return null;

        //
	// B
	//
      case 'b':
	if ( e.equals("boot") ) {
	  Boot b = new Boot();
	  b.addAttributes(names, values);
	  return b;
        } else if ( e.equals("basic") ) {
	  MachineSpecific basic = new MachineSpecific("basic");
	  return basic;
	}

	// unknown
        return null;

	//
	// C
	//
      case 'c':
	if ( e.equals("cwd") ) {
	  WorkingDir cwd = new WorkingDir();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    this.complain( e, name, value );
	  }
	  return cwd;
	} else if ( e.equals("cleanup") ) {
	  Job job = new Job(e);
	  setupJob( job, names, values );
	  return job;
	} else if ( e.equals( "cpu" ) ){
	  CPU c  = new CPU();
	  c.addAttributes(names, values);
	  return c;
        }

	// unknown
	return null;

	//
	// D
	//
      case 'd':
	if ( e.equals("data") ) {
	  Data data = new Data();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("truncated") ) {
	      this.log( e, name, value );
	      data.setTruncated( Boolean.valueOf(value).booleanValue() );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return data;
	} else if ( e.equals("descriptor") ) {
	  Descriptor file = new Descriptor();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("number") ) {
	      this.log( e, name, value );
	      file.setDescriptor( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return file;
	} else if ( e.equals("darwin") ) {
	  MachineSpecific darwin = new MachineSpecific("darwin");
	  return darwin;
	}

	// unknown
	return null;

	//
	// E
	//
      case 'e':
	if ( e.equals("env") ) {
	  EnvEntry ee = new EnvEntry();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("key") ) {
	      this.log( e, name, value );
	      ee.setKey( value );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return ee;

	} else if ( e.equals("environment") ) {
	  Environment env = new Environment();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);
	    this.complain( e, name, value );
	  }
	  return env;
	}

	// unknown
	return null;

	//
	// F
	//
      case 'f':
	if ( e.equals("file") ) {
	  Regular file = new Regular();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("name") ) {
	      this.log( e, name, value );
	      file.setFilename(value);
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return file;
	} else if ( e.equals("fifo") ) {
	  Fifo fifo = new Fifo();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("name") ) {
	      this.log( e, name, value );
	      fifo.setFilename(value);
	    } else if ( name.equals("descriptor") ) {
	      this.log( e, name, value );
	      fifo.setDescriptor( Integer.parseInt(value) );
	    } else if ( name.equals("count") ) {
	      this.log( e, name, value );
	      fifo.setCount( Integer.parseInt(value) );
	    } else if ( name.equals("rsize") ) {
	      this.log( e, name, value );
	      fifo.setInputSize( Long.parseLong(value) );
	    } else if ( name.equals("wsize") ) {
	      this.log( e, name, value );
	      fifo.setOutputSize( Long.parseLong(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return fifo;
	} else if ( e.equals("failure") ) {
	  JobStatusFailure failed = new JobStatusFailure();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("error") ) {
	      this.log( e, name, value );
	      failed.setError( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return failed;
	}

	// unknown
	return null;

	//
	// H
	//
      case 'h':
	if ( e.equals("hard") ) {
	  return new Ignore();
	}

	// unknown
	return null;

	//
	// I
	//
      case 'i':
	if ( e.equals("invocation") ) {
	  this.m_result = new InvocationRecord();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("version") ) {
	      this.log( e, name, value );
	      m_result.setVersion( value );
	    } else if ( name.equals("start") ) {
	      this.log( e, name, value );
	      m_result.setStart( parseDate(value) );
	    } else if ( name.equals("duration") ) {
	      this.log( e, name, value );
	      m_result.setDuration( Double.parseDouble(value) );
	    } else if ( name.equals("transformation") ) {
	      this.log( e, name, value );
	      m_result.setTransformation(value);
	    } else if ( name.equals("derivation") ) {
	      this.log( e, name, value );
	      m_result.setDerivation(value);
	    } else if ( name.equals("host") || name.equals("hostaddr") ) {
	      this.log( e, name, value );
	      m_result.setHostAddress( InetAddress.getByName(value) );
	    } else if ( name.equals("hostname") ) {
	      this.log( e, name, value );
	      m_result.setHostname( value );
	    } else if ( name.equals("interface") ) {
	      this.log( e, name, value );
	      m_result.setInterface( value );
	    } else if ( name.equals("resource") ) {
	      this.log( e, name, value );
	      m_result.setResource( value );
	    } else if ( name.equals("ram" ) ) {
	      this.log( e, name, value );
	      m_result.setPhysicalMemory( Long.parseLong(value) );
	    } else if ( name.equals("pid") ) {
	      this.log( e, name, value );
	      m_result.setPID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
	    } else if ( name.equals("uid") ) {
	      this.log( e, name, value );
	      m_result.setUID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
	    } else if ( name.equals("gid") ) {
	      this.log( e, name, value );
	      m_result.setGID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
	    } else if ( name.equals("user") ) {
	      this.log( e, name, value );
	      m_result.setUser( value );
	    } else if ( name.equals("group") ) {
	      this.log( e, name, value );
	      m_result.setGroup( value );
	    } else if ( name.equals("schemaLocation") ) {
	      // ignore root element schema location hint
	    } else if ( name.equals("umask") ) {
	      this.log( e, name, value );
	      m_result.setUMask( Integer.parseInt(value,8) );
	    } else if ( name.equals("wf-label") ) {
	      this.log( e, name, value );
	      m_result.setWorkflowLabel( value );
	    } else if ( name.equals("wf-stamp") ) {
	      this.log( e, name, value );
	      m_result.setWorkflowTimestamp( parseDate(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return this.m_result;
	}

	// unknown
	return null;

        //
	// L
	//
      case 'l':
	if ( e.equals("load") ) {
	  Load l = new Load();
          l.addAttributes( names, values );
	  return l;
	} else if ( e.equals("linux") ) {
	  MachineSpecific linux = new MachineSpecific("linux");
	  return linux;
	}

	// unknown
	return null;

	//
	// M
	//
      case 'm':
	if ( e.equals("mainjob") ) {
	  Job job = new Job(e);
	  setupJob( job, names, values );
	  return job;
	} else if ( e.equals( "machine" ) ){
	  Machine machine = new Machine();
	  for ( int i=0; i< names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("page-size") ) {
	      this.log( e, name, value );
	      machine.setPageSize( Long.parseLong(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return machine;
        }

	// unknown
	return null;

	//
	// P
	//
      case 'p':
	if ( e.equals("prejob") ) {
	  Job job = new Job(e);
	  setupJob( job, names, values );
	  return job;
	} else if ( e.equals("postjob") ) {
	  Job job = new Job(e);
	  setupJob( job, names, values );
	  return job;
	} else if ( e.equals( "proc") ){
	  Proc p = new Proc();
	  p.addAttributes( names, values );
	  return p;
        }

	// unknown
	return null;

	//
	// R
	//
      case 'r':
	if ( e.equals("regular") ) {
	  JobStatusRegular regular = new JobStatusRegular();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("exitcode") ) {
	      this.log( e, name, value );
	      regular.setExitCode( Short.parseShort(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return regular;
	} else if ( e.equals("resource") ) {
	  // ignore
	  return new Ignore();
	} else if( e.equals( "ram" ) ) {
	  RAM r = new RAM();
	  r.addAttributes( names, values );
	  return r;
        }

	// unknown
	return null;

	//
	// S
	//
      case 's':
	if ( e.equals("statcall") ) {
	  StatCall statcall = new StatCall();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("error") ) {
	      this.log( e, name, value );
	      statcall.setError( Integer.parseInt(value) );
	    } else if ( name.equals("id") ) {
	      this.log( e, name, value );
	      statcall.setHandle(value);
	    } else if ( name.equals("lfn") ) {
	      this.log( e, name, value );
	      statcall.setLFN(value);
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return statcall;

	} else if ( e.equals("statinfo") ) {
	  StatInfo statinfo = new StatInfo();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);
            //System.out.println( name + " -> " + value );
            if ( name.equals("mode") ) {
	      this.log( e, name, value );
	      statinfo.setMode( Integer.parseInt(value,8) );
	    } else if ( name.equals("size") ) {
	      this.log( e, name, value );
	      statinfo.setSize( Long.parseLong(value) );
	    } else if ( name.equals("inode") ) {
	      this.log( e, name, value );
	      statinfo.setINode( (long)Double.parseDouble(value) );
	    } else if ( name.equals("nlink") ) {
	      this.log( e, name, value );
	      statinfo.setLinkCount( Long.parseLong(value) );
	    } else if ( name.equals("blksize") ) {
	      this.log( e, name, value );
	      statinfo.setBlockSize( Long.parseLong(value) );
	    } else if ( name.equals("blocks") ) {
	      this.log( e, name, value );
	      statinfo.setBlocks( Long.parseLong(value) );
	    } else if ( name.equals("atime") ) {
	      this.log( e, name, value );
	      statinfo.setAccessTime( parseDate(value) );
	    } else if ( name.equals("ctime") ) {
	      this.log( e, name, value );
	      statinfo.setCreationTime( parseDate(value) );
	    } else if ( name.equals("mtime") ) {
	      this.log( e, name, value );
	      statinfo.setModificationTime( parseDate(value) );
	    } else if ( name.equals("uid") ) {
	      this.log( e, name, value );
	      statinfo.setUID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
	    } else if ( name.equals("user") ) {
	      this.log( e, name, value );
	      statinfo.setUser( value );
	    } else if ( name.equals("gid") ) {
	      this.log( e, name, value );
	      statinfo.setGID( (int) (Long.parseLong(value) & 0xFFFFFFFF) );
	    } else if ( name.equals("group") ) {
	      this.log( e, name, value );
	      statinfo.setGroup( value );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return statinfo;

	} else if ( e.equals("status") ) {
	  Status status = new Status();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("raw") ) {
	      this.log( e, name, value );
	      status.setStatus( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return status;

	} else if ( e.equals("soft") ) {
	  return new Ignore();

	} else if ( e.equals("signalled") ) {
	  JobStatusSignal signalled = new JobStatusSignal();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("signal") ) {
	      this.log( e, name, value );
	      signalled.setSignalNumber( Short.parseShort(value) );
	    } else if ( name.equals("corefile") ) {
	      this.log( e, name, value );
	      signalled.setCoreFlag( Boolean.valueOf(value).booleanValue() );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return signalled;

	} else if ( e.equals("suspended") ) {
	  JobStatusSuspend suspended = new JobStatusSuspend();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("signal") ) {
	      this.log( e, name, value );
	      suspended.setSignalNumber( Short.parseShort(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return suspended;

	} else if ( e.equals("setup") ) {
	  Job job = new Job(e);
	  setupJob( job, names, values );
	  return job;
	} else if( e.equals( "stamp" ) ){
	  Stamp s = new Stamp();
	  return s;
        } else if( e.equals( "swap" ) ){
	  Swap s = new Swap();
	  s.addAttributes( names, values );
	  return s;

        } else if ( e.equals("sunos") ) {
	  MachineSpecific sunos = new MachineSpecific("sunos");
	  return sunos;
	}

	// unknown
	return null;

	//
	// T
	//
      case 't':
	if ( e.equals("temporary") ) {
	  Temporary file = new Temporary();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("name") ) {
	      this.log( e, name, value );
	      file.setFilename(value);
	    } else if ( name.equals("descriptor") ) {
	      this.log( e, name, value );
	      file.setDescriptor( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return file;
	} else if( e.equals( "task" ) ){
	  Task t = new Task();
	  t.addAttributes(names, values);
	  return t;
        }

	// unknown
	return null;

	//
	// U
	//
      case 'u':
	if ( e.equals("usage") ) {
	  Usage usage = new Usage();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("utime") ) {
	      this.log( e, name, value );
	      usage.setUserTime( Double.parseDouble(value) );
	    } else if ( name.equals("stime") ) {
	      this.log( e, name, value );
	      usage.setSystemTime( Double.parseDouble(value) );
	    } else if ( name.equals("minflt") ) {
	      this.log( e, name, value );
	      usage.setMinorFaults( Integer.parseInt(value) );
	    } else if ( name.equals("majflt") ) {
	      this.log( e, name, value );
	      usage.setMajorFaults( Integer.parseInt(value) );
	    } else if ( name.equals("nswap") ) {
	      this.log( e, name, value );
	      usage.setSwaps( Integer.parseInt(value) );
	    } else if ( name.equals("nsignals") ) {
	      this.log( e, name, value );
	      usage.setSignals( Integer.parseInt(value) );
	    } else if ( name.equals("nvcsw") ) {
	      this.log( e, name, value );
	      usage.setVoluntarySwitches( Integer.parseInt(value) );
	    } else if ( name.equals("nivcsw") ) {
	      this.log( e, name, value );
	      usage.setInvoluntarySwitches( Integer.parseInt(value) );
	    } else if ( name.equals("maxrss") ) {
	      this.log( e, name, value );
	      usage.setMaximumRSS( Integer.parseInt(value) );
	    } else if ( name.equals("ixrss") ) {
	      this.log( e, name, value );
	      usage.setSharedRSS( Integer.parseInt(value) );
	    } else if ( name.equals("idrss") ) {
	      this.log( e, name, value );
	      usage.setUnsharedRSS( Integer.parseInt(value) );
	    } else if ( name.equals("isrss") ) {
	      this.log( e, name, value );
	      usage.setStackRSS( Integer.parseInt(value) );
	    } else if ( name.equals("inblock") ) {
	      this.log( e, name, value );
	      usage.setInputBlocks( Integer.parseInt(value) );
	    } else if ( name.equals("outblock") ) {
	      this.log( e, name, value );
	      usage.setOutputBlocks( Integer.parseInt(value) );
	    } else if ( name.equals("msgsnd") ) {
	      this.log( e, name, value );
	      usage.setSent( Integer.parseInt(value) );
	    } else if ( name.equals("msgrcv") ) {
	      this.log( e, name, value );
	      usage.setReceived( Integer.parseInt(value) );
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return usage;
	} else if( e.equals( "uname" ) && parent instanceof Machine ){
	  Uname u = new Uname();
	  u.addAttributes(names, values);
	  return u;
        } else if ( e.equals("uname") ) {
	  Architecture uname = new Architecture();
	  for ( int i=0; i<names.size(); ++i ) {
	    String name = (String) names.get(i);
	    String value = (String) values.get(i);

	    if ( name.equals("system") ) {
	      this.log( e, name, value );
	      uname.setSystemName(value);
	    } else if ( name.equals("archmode") ) {
	      this.log( e, name, value );
	      uname.setArchMode(value);
	    } else if ( name.equals("nodename") ) {
	      this.log( e, name, value );
	      uname.setNodeName(value);
	    } else if ( name.equals("release") ) {
	      this.log( e, name, value );
	      uname.setRelease(value);
	    } else if ( name.equals("machine") ) {
	      this.log( e, name, value );
	      uname.setMachine(value);
	    } else if ( name.equals("domainname") ) {
	      this.log( e, name, value );
	      uname.setDomainName(value);
	    } else {
	      this.complain( e, name, value );
	    }
	  }
	  return uname;
	}

	// unknown
	return null;

      default:
	// FIXME: shouldn't this be an exception?
	m_log.log( "filler", 0,
		   "Error: No rules defined for element " + e );
	return null;
      }
    } catch ( NumberFormatException nfe ) {
      m_log.log( "filler", 0,
		 "Error: Unable to parse a number: " + nfe.getMessage() +
		 " at " + where() );
      return null;
    } catch ( UnknownHostException uh ) {
      m_log.log( "filler", 0,
		 "Error: Unable to parse a hostname: " + uh.getMessage() +
		 " at " + where() );
      return null;
    } catch ( ParseException pe ) {
      m_log.log( "filler", 0,
		 "Error: Unable to parse a date: " + pe.getMessage() +
		 " at " + where() );
      return null;
    }
  }

  /**
   * This method sets the relations between the currently finished XML
   * element and its containing element in terms of Java objects.
   * Usually it involves adding the object to the parent's child object
   * list.
   *
   * @param initial is the first charactor of the parent element name
   * @param parent is a reference to the parent's Java object
   * @param child is the completed child object to connect to the parent
   * @return true if the element was added successfully, false, if the
   * child does not match into the parent.
   */
  protected boolean setElementRelation( char initial,
					Invocation parent,
					Invocation child )
  {
    switch ( initial ) {
      //
      // A
      //
    case 'a':
      if ( parent instanceof ArgVector && child instanceof ArgEntry ) {
	ArgVector args = (ArgVector) parent;
	ArgEntry entry = (ArgEntry) child;
	args.setValue( entry.getPosition(), entry.getValue() );
	return true;
      }
      // unknown
      return false;

      //
      // C
      //
    case 'c':
      if ( parent instanceof Job ) {
	Job job = (Job) parent;
	if ( child instanceof Usage ) {
	  job.setUsage((Usage) child);
	  return true;
	} else if ( child instanceof Status ) {
	  job.setStatus((Status) child);
	  return true;
	} else if ( child instanceof StatCall ) {
	  job.setExecutable((StatCall) child);
	  return true;
	} else if ( child instanceof Arguments ) {
	  job.setArguments((Arguments) child);
	  return true;
	}
      }
      // unknown
      return false;

      //
      // E
      //
    case 'e':
      if ( parent instanceof Environment && child instanceof EnvEntry ) {
	((Environment) parent).addEntry((EnvEntry) child);
	return true;
      }

      // unknown
      return false;

      //
      // I
      //
    case 'i':
      if ( parent instanceof InvocationRecord ) {
	InvocationRecord invocation = (InvocationRecord) parent;
	if ( child instanceof Job ) {
	  invocation.addJob((Job) child);
	  return true;
	} else if ( child instanceof Usage ) {
	  invocation.setUsage((Usage) child);
	  return true;
	} else if ( child instanceof StatCall ) {
	  invocation.addStatCall((StatCall) child);
	  return true;
	} else if ( child instanceof WorkingDir ) {
	  invocation.setWorkingDirectory((WorkingDir) child);
	  return true;
	} else if ( child instanceof Architecture ) {
	  invocation.setArchitecture((Architecture) child);
	  return true;
	} else if ( child instanceof Environment ) {
	  invocation.setEnvironment((Environment) child);
	  return true;
	} else if ( child instanceof Machine ) {
          Machine machine = (Machine) child;
	  invocation.setMachine(machine);

          // convert uname object to Architecture object
          // reqd for Pegasus Bug 39
	  invocation.setArchitecture( machine.getUname().toArchitecture() );
          return true;
	}
      }
      // unknown
      return false;

      //
      // mainjob
      //
    case 'm':
      if ( parent instanceof Job ) {
	Job job = (Job) parent;
	if ( child instanceof Usage ) {
	  job.setUsage((Usage) child);
	  return true;
	} else if ( child instanceof Status ) {
	  job.setStatus((Status) child);
	  return true;
	} else if ( child instanceof StatCall ) {
	  job.setExecutable((StatCall) child);
	  return true;
	} else if ( child instanceof Arguments ) {
	  job.setArguments((Arguments) child);
	  return true;
	}
      } else if ( parent instanceof Machine ) {
	Machine m = (Machine) parent;
	if ( child instanceof Stamp ) {
	  m.setStamp( (Stamp) child );
	  return true;
	} else if ( child instanceof Uname ) {
	  m.setUname( (Uname) child );
	  return true;
	} else if ( child instanceof MachineSpecific ) {
	  m.setMachineSpecific( (MachineSpecific) child );
	  return true;
	}
      } else if ( parent instanceof MachineSpecific ) {
	MachineSpecific ms = (MachineSpecific) parent;
	if ( child instanceof RAM  ||
	     child instanceof Swap ||
	     child instanceof Boot ||
	     child instanceof CPU  ||
	     child instanceof Load ||
	     child instanceof Proc ||
	     child instanceof Task ) {
	  ms.addMachineInfo( (MachineInfo) child );
	  return true;
	}
      }

      // unknown
      return false;

      //
      // P
      //
    case 'p':
      if ( parent instanceof Job ) {
	// both, prejob and postjob
	Job job = (Job) parent;
	if ( child instanceof Usage ) {
	  job.setUsage((Usage) child);
	  return true;
	} else if ( child instanceof Status ) {
	  job.setStatus((Status) child);
	  return true;
	} else if ( child instanceof StatCall ) {
	  job.setExecutable((StatCall) child);
	  return true;
	} else if ( child instanceof Arguments ) {
	  job.setArguments((Arguments) child);
	  return true;
	}
      }

      // unknown
      return false;

      //
      // R
      //
    case 'r':
      if ( parent instanceof Ignore && child instanceof Ignore ) {
	// ignore
	return true;
      }

      // unknown
      return false;

      //
      // S
      //
    case 's':
      if ( parent instanceof Status && child instanceof JobStatus ) {
	((Status) parent).setJobStatus((JobStatus) child);
	return true;
      } else if ( parent instanceof StatCall ) {
	StatCall statcall = (StatCall) parent;
	if ( child instanceof edu.isi.pegasus.planner.invocation.File ) {
	  statcall.setFile((edu.isi.pegasus.planner.invocation.File) child);
	  return true;
	} else if ( child instanceof StatInfo ) {
	  statcall.setStatInfo((StatInfo) child);
	  return true;
	} else if ( child instanceof Data ) {
	  statcall.setData((Data) child);
	  return true;
	}
      } else if ( parent instanceof Job ) {
	// both, prejob and postjob
	Job job = (Job) parent;
	if ( child instanceof Usage ) {
	  job.setUsage((Usage) child);
	  return true;
	} else if ( child instanceof Status ) {
	  job.setStatus((Status) child);
	  return true;
	} else if ( child instanceof StatCall ) {
	  job.setExecutable((StatCall) child);
	  return true;
	} else if ( child instanceof Arguments ) {
	  job.setArguments((Arguments) child);
	  return true;
	}
      }

      // unknown
      return false;

    default:
      // FIXME: shouldn't this be an exception?
      m_log.log( "filler", 0,
		 "Error: unable to join child to parent" );
      return false;
    }
  }
}
