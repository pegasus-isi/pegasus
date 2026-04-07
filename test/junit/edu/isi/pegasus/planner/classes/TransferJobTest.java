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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TransferJobTest {

    @Test
    public void testDefaultConstructorNonThirdPartySiteIsNull() {
        TransferJob tj = new TransferJob();
        assertThat(tj.getNonThirdPartySite(), nullValue());
    }

    @Test
    public void testSetAndGetNonThirdPartySite() {
        TransferJob tj = new TransferJob();
        tj.setNonThirdPartySite("isi");
        assertThat(tj.getNonThirdPartySite(), is("isi"));
    }

    @Test
    public void testSetNonThirdPartySiteOverwritesPreviousValue() {
        TransferJob tj = new TransferJob();
        tj.setNonThirdPartySite("siteA");
        tj.setNonThirdPartySite("siteB");
        assertThat(tj.getNonThirdPartySite(), is("siteB"));
    }

    @Test
    public void testConstructorFromJobPreservesNonThirdPartySiteAsNull() {
        Job baseJob = new Job();
        TransferJob tj = new TransferJob(baseJob);
        assertThat(tj.getNonThirdPartySite(), nullValue());
    }

    @Test
    public void testConstructorFromJobCopiesInheritedJobFields() {
        Job baseJob = new Job();
        baseJob.jobName = "stage_in_job";
        baseJob.logicalName = "pegasus-transfer";
        baseJob.setSiteHandle("local");

        TransferJob tj = new TransferJob(baseJob);

        assertThat(tj.jobName, is("stage_in_job"));
        assertThat(tj.logicalName, is("pegasus-transfer"));
        assertThat(tj.getSiteHandle(), is("local"));
    }

    @Test
    public void testClonePreservesNonThirdPartySite() {
        TransferJob original = new TransferJob();
        original.setNonThirdPartySite("condorPool");
        TransferJob clone = (TransferJob) original.clone();
        assertThat(clone.getNonThirdPartySite(), is("condorPool"));
    }

    @Test
    public void testCloneIsIndependentObject() {
        TransferJob original = new TransferJob();
        original.setNonThirdPartySite("site1");
        TransferJob clone = (TransferJob) original.clone();
        assertThat(clone, not(sameInstance(original)));
    }

    @Test
    public void testCloneWithNullNonThirdPartySite() {
        TransferJob original = new TransferJob();
        TransferJob clone = (TransferJob) original.clone();
        assertThat(clone.getNonThirdPartySite(), nullValue());
    }

    @Test
    public void testSetNonThirdPartySiteCanBeResetToNull() {
        TransferJob tj = new TransferJob();
        tj.setNonThirdPartySite("site1");

        tj.setNonThirdPartySite(null);

        assertThat(tj.getNonThirdPartySite(), nullValue());
    }

    @Test
    public void testClonePreservesInheritedJobFields() {
        TransferJob original = new TransferJob();
        original.jobName = "transfer_job";
        original.logicalName = "pegasus-transfer";
        original.setSiteHandle("staging-site");
        original.setNonThirdPartySite("compute-site");

        TransferJob clone = (TransferJob) original.clone();

        assertThat(clone.jobName, is("transfer_job"));
        assertThat(clone.logicalName, is("pegasus-transfer"));
        assertThat(clone.getSiteHandle(), is("staging-site"));
        assertThat(clone.getNonThirdPartySite(), is("compute-site"));
    }

    @Test
    public void testToStringContainsNonThirdPartySite() {
        TransferJob tj = new TransferJob();
        tj.logicalName = "transfer-job";
        tj.setNonThirdPartySite("targetSite");
        String str = tj.toString();
        assertThat(str, containsString("targetSite"));
    }

    @Test
    public void testToStringContainsNonTPTLabel() {
        TransferJob tj = new TransferJob();
        tj.logicalName = "transfer-job";
        tj.setNonThirdPartySite("mySite");
        assertThat(tj.toString(), containsString("Non TPT Site"));
    }

    @Test
    public void testToStringShowsNullWhenNonThirdPartySiteUnset() {
        TransferJob tj = new TransferJob();
        tj.logicalName = "transfer-job";

        assertThat(tj.toString(), containsString("Non TPT Site     :null"));
    }
}
