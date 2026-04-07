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
package edu.isi.pegasus.aws.batch.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for {@link PegasusAWSBatchException}. */
public class PegasusAWSBatchExceptionTest {

    @Test
    public void defaultConstructorInitializesWithoutMessageOrNextException() {
        PegasusAWSBatchException exception = new PegasusAWSBatchException();

        assertThat(exception.getMessage(), is(nullValue()));
        assertThat(exception.getCause(), is(nullValue()));
        assertThat(exception.getNextException(), is(nullValue()));
    }

    @Test
    public void stringAndCauseConstructorsRetainPayload() {
        IllegalStateException cause = new IllegalStateException("boom");

        PegasusAWSBatchException withMessage = new PegasusAWSBatchException("failure");
        PegasusAWSBatchException withMessageAndCause =
                new PegasusAWSBatchException("failure", cause);
        PegasusAWSBatchException withCauseOnly = new PegasusAWSBatchException(cause);

        assertThat(withMessage.getMessage(), is("failure"));
        assertThat(withMessage.getCause(), is(nullValue()));

        assertThat(withMessageAndCause.getMessage(), is("failure"));
        assertThat(withMessageAndCause.getCause(), is(cause));

        assertThat(withCauseOnly.getCause(), is(cause));
        assertThat(withCauseOnly.getMessage(), containsString("boom"));
    }

    @Test
    public void setNextExceptionAppendsToEndOfChain() {
        PegasusAWSBatchException root = new PegasusAWSBatchException("root");
        PegasusAWSBatchException second = new PegasusAWSBatchException("second");
        PegasusAWSBatchException third = new PegasusAWSBatchException("third");

        root.setNextException(second);
        root.setNextException(third);

        assertThat(root.getNextException(), is(second));
        assertThat(second.getNextException(), is(third));
        assertThat(third.getNextException(), is(nullValue()));
    }
}
