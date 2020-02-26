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
package edu.isi.pegasus.common.util;

import java.lang.reflect.*;

/**
 * This class provides a dynamic class loading facility. It is tightly coupled to the property
 * facility. To dynamically obtain an instance of a class through its constructor:
 *
 * <pre>
 * Integer i = null;
 * DynamicLoader dl = new DynamicLoader( "java.lang.Integer" );
 * try {
 *   // instantiate as Integer("42")
 *   String arg[] = new String[1];
 *   arg[0] = "42";
 *   i = (Integer) dl.instantiate(arg);
 * } catch ( Exception e ) {
 *   System.err.println( dl.convertException(e) );
 *   System.exit(1);
 * }
 * </pre>
 *
 * Similarily, to obtain an instance of a class through a static method provided by the same class,
 * or another class:
 *
 * <pre>
 * Integer i = null;
 * DynamicLoader dl = new DynamicLoader( "java.lang.Integer" );
 * try {
 *   // instantiate as Integer("42")
 *   String arg[] = new String[1];
 *   arg[0] = "42";
 *   i = (Integer) dl.static_method( "valueOf", arg );
 * } catch ( Exception e ) {
 *   System.err.println( dl.convertException(e) );
 *   System.exit(1);
 * }
 * </pre>
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class DynamicLoader {
    /** Stores the fully qualified class name to dynamically instantiate. */
    private String m_classname;

    /** */
    public DynamicLoader(String classname) {
        if ((this.m_classname = classname) == null)
            throw new NullPointerException("You must specify a fully-qualified class name");
    }

    /**
     * Sets the fully-qualified class name to load.
     *
     * @param classname is the new class name.
     * @see #getClassName()
     */
    public void setClassName(String classname) {
        if ((this.m_classname = classname) == null)
            throw new NullPointerException("You must specify a fully-qualified class name");
    }

    /**
     * Obtains the fully-qualified class name that this instance works with.
     *
     * @return the class name.
     * @see #setClassName( String )
     */
    public String getClassName() {
        return this.m_classname;
    }

    /**
     * Dynamically instantiates a class from a contructor. You must have set the class name before
     * invoking this method. Please note that any exceptions thrown by the constructor will be
     * wrapped into a <code>InvocationTargetException</code>.
     *
     * @param arguments are arguments to the constructor of the class to load. Please use "new
     *     Object[0]" for the argumentless default constructor.
     * @return an instance that must be cast to the correct class.
     * @exception ClassNotFoundException if the driver for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the driver's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the driver class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the driver class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the driver throws an exception
     *     while being dynamically loaded.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     * @see #setClassName( String )
     */
    public Object instantiate(Object[] arguments)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        // generate class array and populate with class of each argument
        Class[] temp = new Class[arguments.length];
        for (int i = 0; i < arguments.length; ++i) temp[i] = arguments[i].getClass();

        // load class into memory and obtain an instance of it
        return Class.forName(m_classname).getConstructor(temp).newInstance(arguments);
    }

    /**
     * Dynamically instantiates a class from a contructor. You must have set the class name before
     * invoking this method. Please note that any exceptions thrown by the constructor will be
     * wrapped into a <code>InvocationTargetException</code>.
     *
     * <p>This method should be invoked, if the constructor declares interface types as formal
     * arguments, but the actual arguments are implementation classes.
     *
     * @param classes is a vector of the classes involved. Each item in the classes vector matches
     *     the item in the arguments vector. The classes vector will be used to select the correct
     *     constructor. Please use "new Class[0]" for the argumentless default ctor.
     * @param arguments are arguments to the constructor of the class to load. Please use "new
     *     Object[0]" for the argumentless default constructor.
     * @return an instance that must be cast to the correct class.
     * @exception ClassNotFoundException if the driver for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the driver's constructor interface does not comply with
     *     the database driver API.
     * @exception IllegalArgumentException is thrown, if the number of arguments do not match the
     *     number of types, ie the vector have mismatching sizes.
     * @exception InstantiationException if the driver class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the driver class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the driver throws an exception
     *     while being dynamically loaded.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     * @see #setClassName( String )
     */
    public Object instantiate(Class[] classes, Object[] arguments)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        // complain on argument mismatch
        if (classes.length != arguments.length)
            throw new IllegalArgumentException("vector sizes must match");

        // load class into memory and obtain an instance of it
        return Class.forName(m_classname).getConstructor(classes).newInstance(arguments);
    }

    /**
     * Dynamically instantiates a class from a static method which constructs the resulting object.
     * You must have set the class name before invoking this method. Please note that any exceptions
     * thrown by the constructor will be wrapped into a <code>InvocationTargetException</code>.
     *
     * @param method is the name of the static method to call.
     * @param arguments are arguments to the constructor of the class to load. Please use "new
     *     Object[0]" for the argumentless default constructor.
     * @return an instance that must be cast to the correct class.
     * @exception ClassNotFoundException if the driver for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the driver's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the driver class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the driver class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the driver throws an exception
     *     while being dynamically loaded.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     * @exception SecurityException if you are not permitted to invoke the method, or even list the
     *     methods provided by the class.
     * @exception NullPointerException if the method name is <code>null</code>.
     * @exception IllegalArgumentException if the number of actual and formal parameter differ,
     *     unwrapping a primitive type failed, or a parameter value cannot be converted to the
     *     formal argument type.
     * @see #setClassName( String )
     */
    public Object static_method(String method, Object[] arguments)
            throws ClassNotFoundException, SecurityException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    NullPointerException, IllegalArgumentException {
        // generate class array and populate with class of each argument
        Class[] temp = new Class[arguments.length];
        for (int i = 0; i < arguments.length; ++i) temp[i] = arguments[i].getClass();

        // load the class into memory, and find the method, and invoke it as
        // a static method
        return Class.forName(m_classname).getDeclaredMethod(method, temp).invoke(null, arguments);
    }

    /**
     * Converts an exception from the class loader into an error message.
     *
     * @param classname is the name or some other class signifier.
     * @param e is the exception thrown by the class loader.
     * @return a string that tries to describe what went wrong.
     */
    public static String convertException(String classname, Exception e) {
        String result = null;

        // check exceptions
        result = convertExceptionToString(classname, e);

        // Commented out, as defined in convertToString(String,Exception) function
        // Karan April 25, 2006
        //    if ( e instanceof ClassNotFoundException ) {
        //      result = "Unable to dynamically load " + classname;
        //      // do cause
        //    } else if ( e instanceof NoSuchMethodException ) {
        //      result = "Unable to dynamically invoke the constructor of " + classname;
        //      // no cause
        //    } else if ( e instanceof InstantiationException ) {
        //      result = "The dynamically loadable class " + classname + " is either " +
        //	"abstract or an interface";
        //      // no cause
        //    } else if ( e instanceof IllegalAccessException ) {
        //      result = "Unable to access appropriate constructor in " + classname;
        //      // no cause
        //    } else if ( e instanceof InvocationTargetException ) {
        //      result = "Class " + classname + " threw exception " +
        //	e.getClass().getName() + " during construction";
        //      // do cause
        //    } else if ( e instanceof IllegalArgumentException ) {
        //      result = "Class " + classname + " threw exception " +
        //	e.getClass().getName() + " during method invocation argument " +
        //	"list construction";
        //    } else if ( e instanceof NullPointerException ) {
        //      result = "Invalid static initializer method name for " + classname;
        //      // no cause
        //    } else if ( e instanceof SecurityException ) {
        //      result = "Prohibited access to " + classname;
        //      // ?? cause
        //    } else {
        //      result = classname + " caugth " + e.getClass().getName();
        //      // ?? cause
        //    }

        // append complete cause chain
        int i = 0;
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            result += " [" + Integer.toString(++i) + "]: " + cause;
        }

        // done
        return result;
    }

    /**
     * Converts an exception from the class loader into an error message.
     *
     * @param e is the exception thrown by the class loader.
     * @return a string that tries to describe what went wrong.
     */
    public String convertException(Exception e) {
        return DynamicLoader.convertException(m_classname, e);
    }

    /**
     * Converts an exception from the class loader into an error message. Note: It does not convert
     * any cause messages.
     *
     * @param classname is the name or some other class signifier.
     * @param e is the exception thrown by the class loader.
     * @return a string that tries to describe what went wrong.
     */
    public static String convertExceptionToString(String classname, Throwable e) {
        String result = null;

        // check exceptions
        if (e instanceof ClassNotFoundException) {
            result = "Unable to dynamically load " + classname;
            // do cause
        } else if (e instanceof NoSuchMethodException) {
            result = "Unable to dynamically invoke the constructor " + e.getMessage();
            // no cause
        } else if (e instanceof InstantiationException) {
            result =
                    "The dynamically loadable class "
                            + classname
                            + " is either "
                            + "abstract or an interface";
            // no cause
        } else if (e instanceof IllegalAccessException) {
            result = "Unable to access appropriate constructor in " + classname;
            // no cause
        } else if (e instanceof InvocationTargetException) {
            result =
                    "Class "
                            + classname
                            + " threw exception "
                            + e.getClass().getName()
                            + " during construction";
            // do cause
        } else if (e instanceof IllegalArgumentException) {
            result =
                    "Class "
                            + classname
                            + " threw exception "
                            + e.getClass().getName()
                            + " during method invocation argument "
                            + "list construction";
        } else if (e instanceof NullPointerException) {
            result = "Invalid static initializer method name for " + classname;
            // no cause
        } else if (e instanceof SecurityException) {
            result = "Prohibited access to " + classname;
            // ?? cause
        } else {
            result = classname + " caught " + e.getClass().getName() + " " + e.getMessage();
            // ?? cause
        }

        // done
        return result;
    }
}
