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

package edu.isi.pegasus.common.logging;

/**
 * Some predifined logging keys to be used for logging.
 * 
 * @author Karan Vahi
 * @author gmehta
 * 
 * @version $Revision$
 */
public  class LoggingKeys { 
 
    public static final String EVENT_ID_KEY = "eventId";
    public static final String PORTFOLIO_ID = "portfolio.id";
    public static final String REQUEST_ID   = "request.id";
    public static final String DAX_ID       = "dax.id";
    public static final String DAG_ID       = "dag.id";
    public static final String JOB_NUMBER   = "job.id";
    public static final String JOB_ID       = "job.id";
    public static final String SEED_ID      = "seed.id";
    public static final String TEMPLATE_ID  = "template.id";
    public static final String SEED_NAME    = "seed.name";
    
    public static final String EVENT_QUERY_PROCESSCATALOG = "event.query.processcatalog";
    public static final String EVENT_QUERY_DATACATALOG    = "event.query.datacatalog";
	
    
    public static String EVENT_PEGASUS_RANKING = "event.ranking";
    public static String EVENT_PEGASUS_PLAN = "event.pegasus.plan";
    public static String EVENT_PEGASUS_REDUCE = "event.pegasus.reduce";
    public static String EVENT_PEGASUS_SITESELECTION = "event.pegasus.siteselection";
    public static String EVENT_PEGASUS_ADD_TRANSFER_NODES = "event.pegasus.add-transfer-nodes";
    public static String EVENT_PEGASUS_ADDCLEANUP = "event.pegasus.cleanup";
    public static String EVENT_PEGASUS_CLUSTER = "event.pegasus.cluster";
    public static String EVENT_PEGASUS_GENERATECLEANUP = "event.pegasus.generatecleanup";
	
    public static String QUERY_NUMBER = "query.number";
    public static String QUERY_NAME = "query.name";
    public static String QUERY_INPUT = "query.input";
    public static String QUERY_OUTPUT = "query.output";
    public static String QUERY_ID = "query.id";
    public static String QUERY_ARGUMENTS = "query.arguments";
    public static String QUERY_RESPONSE = "query.response";
    public static String FILE_OUTPUT_NAME = "file.output.name";
    public static String FILE_OUTPUT_CONTENTS = "file.output.contents";
    public static String FILE_PRIORTY = "file.priorty";
    public static String FILE_TYPE = "file.type";
    public static String TIME_START = "time.start";
    public static String TIME_END = "time.end";
    public static String SYSTEM_HOSTNAME = "system.hostname";
    public static String SYSTEM_HOSTADDR = "system.hostaddr";
    public static String SYSTEM_OS = "system.os";
    public static String SYSTEM_ARCHITECTURE = "system.architecture";
    public static String SYSTEM_NODENAME = "system.nodename";
    public static String SYSTEM_NUMBEROFPROCESSORS = "system.numberOfProcessors";
    public static String JOB_EXITTCODE = "job.exittcode";
    public static String JOB_ARGUMENTS = "job.arguments";
    public static String JOB_ENVIRONMENTVARIABLE = "job.environmentVariable";
    public static String JOB_RESOURCE_INFORMATION = "job.resource.information";
    public static String PERFMETRIC_CPU_UTILIZATION = "perfmetric.cpu.utilization";
    public static String PERFMETRIC_MEMORY_UTILIZATION = "perfmetric.memory.utilization";
    public static String PERFMETRIC_NETWORK_BANDWIDTH = "perfmetric.network.bandwidth";
    public static String PERFMETRIC_TIME_DURATION = "perfmetric.time.duration";
    
    
}
