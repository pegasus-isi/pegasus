package edu.isi.ikcap.workflows.util.logging;

/**
 * Defines keys for creating logs within the workflow system
 *
 * @author pgroth
 */
public interface LoggingKeys {
    public static String MSG_ID = "msgid";
    public static String EVENT_ID_KEY = "eventId";
    public static String PORTFOLIO_ID = "portfolio.id";
    public static String REQUEST_ID = "request.id";
    public static String DAX_ID = "dax.id";
    public static String DAG_ID = "dag.id";
    public static String JOB_NUMBER = "job.id";
    public static String JOB_ID = "job.id";
    public static String JOB_INPUTS = "job.input.ids";
    public static String JOB_OUTPUTS = "job.output.ids";
    public static String SEED_ID = "seed.id";
    public static String TEMPLATE_ID = "template.id";
    public static String SEED_NAME = "seed.name";
    public static String ONTOLOGY_LOCATION = "ontology.location";

    public static String EVENT_QUERY_PROCESSCATALOG = "event.query.processcatalog";
    public static String EVENT_QUERY_DATACATALOG = "event.query.datacatalog";

    public static String EVENT_WG = "event.wg";
    public static String EVENT_WG_LOAD_SEED = "event.wg.loadseed";
    public static String EVENT_WG_INITIALIZE_PC = "event.wg.pcinitialize";
    public static String EVENT_WG_INITIALIZE_DC = "event.wg.dcinitialize";
    public static String EVENT_WG_GET_CANDIDATE_SEEDS = "event.wg.getcandidateseeds";
    public static String EVENT_WG_BACKWARD_SWEEP = "event.wg.backwardsweep";
    public static String EVENT_WG_SPECIALIZE = "event.wg.specializetemplate";
    public static String EVENT_WG_DATA_SELECTION = "event.wg.dataselection";
    public static String EVENT_WG_FETCH_METRICS = "event.wg.fetchmetrics";
    public static String EVENT_WG_FORWARD_SWEEP = "event.wg.forwardsweep";
    public static String EVENT_WG_CONFIGURE = "event.wg.configuretemplate";
    public static String EVENT_WG_GET_DAX = "event.wg.getdax";

    public static String EVENT_INFER_TEMPLATE = "event.matching.infertemplate";

    public static String DOMAIN = "domain";
    public static String SEED = "seed";
    public static String TEMPLATE = "template";
    public static String NO_MATCH = "No Match";
    public static String QUEUED_TEMPLATES = "templates.queue";
    public static String SPECIALIZED_TEMPLATES_Q = "templates.specialized.queue";
    public static String CONFIGURED_TEMPLATES_Q = "templates.configured.queue";

    public static String EVENT_RANKING = "event.ranking";
    public static String EVENT_PEGASUS_PLAN = "event.pegasus.plan";
    public static String EVENT_PEGASUS_REDUCE = "event.pegasus.reduce";
    public static String EVENT_PEGASUS_SITESELECTION = "event.pegasus.siteselection";
    public static String EVENT_PEGASUS_ADDDATASTAGING = "event.pegasus.adddatastaging";
    public static String EVENT_PEGASUS_ADDREGISTRATION = "event.pegasus.addregistration";
    public static String EVENT_PEGASUS_ADDCLEANUP = "event.pegasus.addcleanup";
    public static String EVENT_PEGASUS_CLUSTER = "event.pegasus.cluster";
    public static String EVENT_PEGASUS_GENERATECLEANUP = "event.pegasus.generatecleanup";

    public static final String DATA_CHARACTERIZATION_PROGRAM = "DataCharacterization";

    public static String EVENT_DC_CHARACTERIZE = "event.dc.characterize";
    public static String EVENT_DC_CHARACTERIZE_STATUS = "event.dc.characterize.status";
    public static String EVENT_DC_CHARACTERIZE_STATUSES = "event.dc.characterize.statuses";

    public static String EVENT_DC_RICH_CHARACTERIZE = "event.dc.richcharacterize";

    public static String EVENT_DC_GET_DATASOURCES = "event.dc.get.datasources";
    public static String EVENT_DC_GET_ALL_METRICS = "event.dc.get.all.metrics";
    public static String EVENT_DC_GET_DMO_METRICS = "event.dc.get.dmo.metrics";

    public static String EVENT_DC_DISSEMINATE_DATASOURCE = "event.dc.disseminate.datasource";

    public static String EVENT_DC_INGEST_DATASOURCE = "event.dc.ingest.datasource";
    public static String EVENT_DC_GET_INGEST_STATUS = "event.dc.get.ingest.status";
    public static String EVENT_DC_UPDATE_INGEST_MAP = "event.dc.update.ingest.map";
    public static String EVENT_DC_GET_INGEST_TIME = "event.dc.get.ingest.time";

    public static String EVENT_DC_REGISTER_DATASOURCE = "event.dc.register.datasource";
    public static String EVENT_DC_GET_ALL_DATASOURCES = "event.dc.getall.datasources";
    public static String EVENT_DC_GET_UNCHARACTERIZED_DATASOURCES =
            "event.dc.getuncharacterized.datasources";

    public static String EVENT_DC_GET_DATASOURCE = "event.dc.get.datasource";
    public static String EVENT_DC_REMOVE_DATASOURCE = "event.dc.remove.datasource";
    public static String EVENT_DC_DATASOURCE_EXISTS = "event.dc.datasource.exists";

    public static String EVENT_DC_GET_DATASOURCE_UUID_FOR_DESCRIPTION =
            "event.dc.get.datasource.uuid.for.description";
    public static String EVENT_DC_GET_DATASOURCE_DESCRIPTION_FOR_UUID =
            "event.dc.get.datasource.description.for.uuid";
    public static String EVENT_DC_SET_DATASOURCE_DESCRIPTION_FOR_UUID =
            "event.dc.set.datasource.description.for.uuid";

    public static String EVENT_DC_AUGMENT_DATASOURCE = "event.dc.augment.datasource";
    public static String EVENT_DC_GET_AUGMENTED_TYPES = "event.dc.get.augmented.types";

    public static String EVENT_DC_SATURATE_DATASOURCE = "event.dc.saturate.datasource";
    public static String EVENT_DC_GET_SATURATED_TYPES = "event.dc.get.saturated.types";

    public static String EVENT_DC_GET_DATASOURCE_LOCATIONS = "event.dc.get.datasourcelocations";
    public static String EVENT_DC_ADD_DATASOURCE_LOCATION = "event.dc.add.datasourcelocation";
    public static String EVENT_DC_REMOVE_DATASOURCE_LOCATION = "event.dc.remove.datasourcelocation";

    public static String MSG = "msg";
    public static String PROG = "prog";
    public static String PROG_SETUP = "prog.setup";
    public static String CATALOG_URL = "catalog.url";

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
    public static String JOB_EXITCODE = "job.exitcode";
    public static String JOB_ARGUMENTS = "job.arguments";
    public static String JOB_ENVIRONMENTVARIABLE = "job.environmentVariable";
    public static String JOB_RESOURCE_INFORMATION = "job.resource.information";
    public static String PERFMETRIC_CPU_UTILIZATION = "perfmetric.cpu.utilization";
    public static String PERFMETRIC_MEMORY_UTILIZATION = "perfmetric.memory.utilization";
    public static String PERFMETRIC_NETWORK_BANDWIDTH = "perfmetric.network.bandwidth";
    public static String PERFMETRIC_TIME_DURATION = "perfmetric.time.duration";
}
