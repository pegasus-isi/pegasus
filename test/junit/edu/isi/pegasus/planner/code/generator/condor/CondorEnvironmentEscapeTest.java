/*
 * Copyright 2007-2015 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.planner.namespace.ENV;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * To test the Condor Environment Escaping logic implemented in Pegasus.
 * 
 * @author Karan Vahi
 */
public class CondorEnvironmentEscapeTest {
    private CondorEnvironmentEscape mEscape = null;

    public CondorEnvironmentEscapeTest() {
    }

    @Before
    public void setUp() {
        mEscape = new CondorEnvironmentEscape();
    }
    
    /**
     * Enclose the whole thing with quotes. 
     */
    @Test
    public void testBasic() {
        Map<String,String> m = new LinkedHashMap();
        m.put( "one", "1" );
        m.put( "two", "2");
        m.put( "three", "3");
        ENV env = new ENV( m );
        
        String expected = "\"one=1 two=2 three=3\"";
        String result  = mEscape.escape(env); 
        assertEquals( expected, result ) ;
    }
    
    @Test
    public void testSpaceInValue(){
        String value = "Escaped Value";
        String expected = "'Escaped Value'";
        String result = mEscape.escape(value);
        assertEquals( expected, result ) ;
    }
    
    @Test
    public void testSingleQuoteInValue(){
        String value = "'SingleQuoted'";
        String expected = "''SingleQuoted''";
        String result = mEscape.escape(value);
        assertEquals( expected, result ) ;
    }
    
    @Test
    public void testDoubleQuoteInValue(){
        String value = "\"DoubleQuoted\"";
        String expected = "\"\"DoubleQuoted\"\"";;
        String result = mEscape.escape(value);
        assertEquals( expected, result ) ;
    }
    
    @Test
    public void testDoubleQuoteWithSpaceInValue(){
        String value = "\"Double Quoted\"";
        String expected = "'\"\"Double Quoted\"\"'";;
        String result = mEscape.escape(value);
        assertEquals( expected, result ) ;
    }
    
    
    /**
     * Test the case described here
     * 
     * http://research.cs.wisc.edu/htcondor/manual/v8.2/condor_submit.html 
     */
    @Test
    public void testCondorExampleInDocumentation() {
        Map<String,String> m = new LinkedHashMap();
        m.put( "one", "1" );
        m.put( "two", "\"2\"");
        m.put( "three", "spacey 'quoted' value");
        ENV env = new ENV( m );
        
        //should print out "one=1 two=""2"" three='spacey ''quoted'' value'"
        String expected = "\"one=1 two=\"\"2\"\" three='spacey ''quoted'' value'\"";
        String result  = mEscape.escape(env); 
        assertEquals( expected, result ) ;
    }
    
}
