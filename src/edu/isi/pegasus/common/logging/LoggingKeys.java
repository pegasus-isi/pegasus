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
	
    
    
    public static final String EVENT_ENSEMBLE_RANKING      = "event.ensemble.ranking";
    public static final String EVENT_ENSEMBLE_PLANNING      = "event.ensemble.planning";
    public static final String EVENT_ENSEMBLE_WG      =         "event.ensemble.wings";
    public static final String EVENT_ENSEMBLE_EXECUTE      = "event.ensemble.workflow.execute";

    public static final String EVENT_PEGASUS_RANKING              = "event.ranking";
    public static final String EVENT_PEGASUS_RANKING_RETRIEVE_DAX = "event.ranking.retrive.dax";
    
    public static final String EVENT_PEGASUS_REFINEMENT           = "event.pegasus.refinement";
    public static final String EVENT_PEGASUS_AUTHENTICATION       = "event.pegasus.authenticate";
    public static final String EVENT_PEGASUS_PLAN                 = "event.pegasus.plan";
    public static final String EVENT_PEGASUS_REDUCE               = "event.pegasus.reduce";
    public static final String EVENT_PEGASUS_SITESELECTION        = "event.pegasus.siteselection";
    public static final String EVENT_PEGASUS_ADD_TRANSFER_NODES   = "event.pegasus.generate.transfer-nodes";
    public static final String EVENT_PEGASUS_CLUSTER              = "event.pegasus.cluster";
    public static final String EVENT_PEGASUS_PARTITION            = "event.pegasus.partition";
    public static final String EVENT_PEGASUS_GENERATE_CLEANUP     = "event.pegasus.generate.cleanup-nodes";
    public static final String EVENT_PEGASUS_GENERATE_CLEANUP_WF  = "event.pegasus.generate.cleanup-wf";
    public static final String EVENT_PEGASUS_GENERATE_WORKDIR     = "event.pegasus.generate.workdir-nodes";     
    public static final String EVENT_PEGASUS_CODE_GENERATION      = "event.pegasus.code.generation";
    public static final String EVENT_PEGASUS_LOAD_TRANSIENT_CACHE = "event.pegasus.load.cache";
    public static final String EVENT_PEGASUS_LOAD_DIRECTORY_CACHE = "event.pegasus.load.directory";
    public static final String EVENT_PEGASUS_PARSE_SITE_CATALOG   = "event.pegasus.parse.site-catalog";
    public static final String EVENT_PEGASUS_PARSE_DAX            = "event.pegasus.parse.dax";
    public static final String EVENT_PEGASUS_PARSE_PDAX           = "event.pegasus.parse.pdax";
    public static final String EVENTS_PEGASUS_STAMPEDE_GENERATION = "event.pegasus.stampede.events";
    public static final String EVENTS_PEGASUS_CODE_GENERATION     = "event.pegasus.code.generation";
    public static final String EVENT_PEGASUS_ADD_DATA_DEPENDENCIES= "event.pegasus.add.data-dependencies";
    public static final String EVENT_PEGASUS_CYCLIC_DEPENDENCY_CHECK = "event.pegasus.check.cyclic-dependencies";
    
    public static final String EVENT_WORKFLOW_JOB_STATUS           = "event.workflow.job.status";

    
    public static final String QUERY_NUMBER = "query.number";
    public static final String QUERY_NAME = "query.name";
    public static final String QUERY_INPUT = "query.input";
    public static final String QUERY_OUTPUT = "query.output";
    public static final String QUERY_ID = "query.id";
    public static final String QUERY_ARGUMENTS = "query.arguments";
    public static final String QUERY_RESPONSE = "query.response";
    public static final String FILE_OUTPUT_NAME = "file.output.name";
    public static final String FILE_OUTPUT_CONTENTS = "file.output.contents";
    public static final String FILE_PRIORTY = "file.priorty";
    public static final String FILE_TYPE = "file.type";
    public static final String TIME_START = "time.start";
    public static final String TIME_END = "time.end";
    public static final String SYSTEM_HOSTNAME = "system.hostname";
    public static final String SYSTEM_HOSTADDR = "system.hostaddr";
    public static final String SYSTEM_OS = "system.os";
    public static final String SYSTEM_ARCHITECTURE = "system.architecture";
    public static final String SYSTEM_NODENAME = "system.nodename";
    public static final String SYSTEM_NUMBEROFPROCESSORS = "system.numberOfProcessors";
    public static final String JOB_EXITTCODE = "job.exittcode";
    public static final String JOB_ARGUMENTS = "job.arguments";
    public static final String JOB_ENVIRONMENTVARIABLE = "job.environmentVariable";
    public static final String JOB_RESOURCE_INFORMATION = "job.resource.information";
    public static final String PERFMETRIC_CPU_UTILIZATION = "perfmetric.cpu.utilization";
    public static final String PERFMETRIC_MEMORY_UTILIZATION = "perfmetric.memory.utilization";
    public static final String PERFMETRIC_NETWORK_BANDWIDTH = "perfmetric.network.bandwidth";
    public static final String PERFMETRIC_TIME_DURATION = "perfmetric.time.duration";
   
    
    
    
}
