/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.griphyn.logsummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
/**
 * Data structure to store the information about the failed job. 
 *
 * @author Atul Kumar
 *
 * @version $Revision: 554 $
 */
/**
 * @author akumar
 *
 */
public class JobStateMeasurement {
	/**
	 * Failed jobs list
	 */
	private List failedJobs = new ArrayList();
	
	/**
	 * metadata about the failed jobs.
	 */
	private Map metadata; 
	
	/**
	 * @return the meta data information about failed jobs
	 */
	public Map getMetadata() {
		if(metadata == null){
			metadata = new HashMap();
		}
		return metadata;
	}
	/**
	 * @param metadata the meta data information about failed jobs
	 */
	public void setMetadata(Map metadata) {
		this.metadata = metadata;
	}
	/**
	 * @return list of failed job
	 */
	public List getFailedJobs() {
		return failedJobs;
	}
	
	/**
	 * @param jobname job name
	 */
	public void addFailedJob(String jobname){
		Iterator itr = failedJobs.iterator();
		for(; itr.hasNext();){
			if(((String)itr.next()).equals(jobname)){
				return;
			}
		}
		failedJobs.add(jobname);
	}
	
}
