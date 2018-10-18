/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author vahi
 */
public class ContainerTest {
    
    private TestSetup mTestSetup;
    
    
    public ContainerTest() {
    }
    
    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
       
        mTestSetup.setInputDirectory( this.getClass() );
    }
    
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    
    @After
    public void tearDown() {
    }

    
    
    @Test
    public void testSingularityFileCVMFS() {
        this.testSingulartiy( "test", "test", "file:///cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:latest" );
    }
    
    @Test
    public void testSingulartiyHUB() {
        this.testSingulartiy( "test", "test.simg",  "shub://pegasus-isi/montage-workflow-v2" ); 
    }
    
    @Test
    public void testSingulartiyHTTPImg() {
        this.testSingulartiy( "test", "test.img", "http:///pegasus.isi.edu/images/singularity/centos-7.img" );
    }
    
    @Test
    public void testSingulartiyHTTPSImg() {
        this.testSingulartiy( "test", "test.simg", "http:///pegasus.isi.edu/images/singularity/centos-7.simg" );
    }
    
    @Test
    public void testSingulartiyHTTPTar() {
        this.testSingulartiy( "test", "test.tar", "http:///pegasus.isi.edu/images/singularity/centos-7.tar" );
    }
    
    @Test
    public void testSingulartiyHTTPTarGZ() {
        this.testSingulartiy( "test", "test.tar.gz", "http:///pegasus.isi.edu/images/singularity/centos-7.tar.gz" );
    }
    
    @Test
    public void testSingulartiyHTTPTarBZ() {
        this.testSingulartiy( "test", "test.tar.bz2", "http:///pegasus.isi.edu/images/singularity/centos-7.tar.bz2" );
    }
    
    @Test
    public void testSingulartiyHTTPCPIO() {
        this.testSingulartiy( "test", "test.cpio", "http:///pegasus.isi.edu/images/singularity/centos-7.cpio" );
    }
    
    @Test
    public void testSingulartiyHTTPTarCPIO() {
        this.testSingulartiy( "test", "test.cpio.gz", "http:///pegasus.isi.edu/images/singularity/centos-7.cpio.gz" );
    }
    
    
    public void testSingulartiy( String name, String expectedLFN, String url ) {
        Container c = new Container( name ) ;
        c.setType( Container.TYPE.singularity ) ;
        String lfn = c.computeLFN( new PegasusURL( url ));
        assertEquals(  expectedLFN, lfn );
    }
    
    
}
