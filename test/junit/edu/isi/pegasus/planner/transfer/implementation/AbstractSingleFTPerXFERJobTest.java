/**
 * Copyright 2007-2013 University Of Southern California
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
package edu.isi.pegasus.planner.transfer.implementation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AbstractSingleFTPerXFERJobTest {

    @Test
    public void testAbstractSingleFTPerXFERJobIsAbstractAndExtendsAbstract() {
        assertThat(Modifier.isAbstract(AbstractSingleFTPerXFERJob.class.getModifiers()), is(true));
        assertThat(AbstractSingleFTPerXFERJob.class.getSuperclass(), is(Abstract.class));
        assertThat(AbstractSingleFTPerXFERJob.class.getInterfaces().length, is(1));
        assertThat(
                AbstractSingleFTPerXFERJob.class.getInterfaces()[0],
                is(edu.isi.pegasus.planner.transfer.SingleFTPerXFERJob.class));
    }

    @Test
    public void testConstructorSignature() throws Exception {
        Constructor<AbstractSingleFTPerXFERJob> constructor =
                AbstractSingleFTPerXFERJob.class.getDeclaredConstructor(PegasusBag.class);

        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
        assertThat(constructor.getParameterCount(), is(1));
        assertThat(AbstractSingleFTPerXFERJob.class.getDeclaredConstructors().length, is(1));
    }

    @Test
    public void testCreateTransferJobOverloadSignatures() throws Exception {
        Method collectionOverload =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod(
                        "createTransferJob",
                        Job.class,
                        String.class,
                        Collection.class,
                        Collection.class,
                        String.class,
                        int.class);
        assertThat(collectionOverload.getReturnType(), is(TransferJob.class));
        assertThat(Modifier.isPublic(collectionOverload.getModifiers()), is(true));

        Method singleFileOverload =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod(
                        "createTransferJob",
                        Job.class,
                        String.class,
                        FileTransfer.class,
                        Collection.class,
                        String.class,
                        int.class);
        assertThat(singleFileOverload.getReturnType(), is(TransferJob.class));
        assertThat(Modifier.isPublic(singleFileOverload.getModifiers()), is(true));
    }

    @Test
    public void testAbstractHelperMethodSignatures() throws Exception {
        Method derivationNamespace =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod("getDerivationNamespace");
        assertThat(derivationNamespace.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(derivationNamespace.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(derivationNamespace.getModifiers()), is(true));

        Method derivationName =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod("getDerivationName");
        assertThat(derivationName.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(derivationName.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(derivationName.getModifiers()), is(true));

        Method derivationVersion =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod("getDerivationVersion");
        assertThat(derivationVersion.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(derivationVersion.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(derivationVersion.getModifiers()), is(true));

        Method generateArgs =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod(
                        "generateArgumentStringAndAssociateCredentials",
                        TransferJob.class,
                        FileTransfer.class);
        assertThat(generateArgs.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(generateArgs.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(generateArgs.getModifiers()), is(true));

        Method completeTCName =
                AbstractSingleFTPerXFERJob.class.getDeclaredMethod("getCompleteTCName");
        assertThat(completeTCName.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(completeTCName.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(completeTCName.getModifiers()), is(true));
    }
}
