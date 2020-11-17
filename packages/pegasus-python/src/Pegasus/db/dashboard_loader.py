__author__ = "Monte Goode"
__author__ = "Karan Vahi"

import time

from sqlalchemy import exc

from Pegasus.db.base_loader import BaseLoader
from Pegasus.db.schema import *


class DashboardLoader(BaseLoader):

    MAX_RETRIES = 10  # maximum number of retries in case of operational errors that arise because of database locked/connection dropped

    """Load into the Stampede Dashboard SQL schema through SQLAlchemy.

    Parameters:
      - connString {string,None*}: SQLAlchemy connection string.
        The general form of this is
          'dialect+driver://username:password@host:port/database'.
        See the SQLAlchemy docs for details.
        For sqlite, use 'sqlite:///foo.db' for a relative path and
        'sqlite:////path/to/foo.db' (four slashes) for an absolute one.
        When using MySQL, the general form will work, but the library
        expects the database to exist (ie: will not issue CREATE DB)
        but will populate an empty DB with tables/indexes/etc.
    """

    def __init__(
        self,
        connString,
        perf=False,
        batch=False,
        props=None,
        db_type=None,
        backup=False,
    ):
        """Init object

        @type   connString: string
        @param  connString: SQLAlchemy connection string - REQUIRED
        """
        super().__init__(
            connString,
            batch=batch,
            props=props,
            db_type=db_type,
            backup=backup,
            flush_every=1,
        )

        # "Case" dict to map events to handler methods
        self.eventMap = {
            "dashboard.wf.plan": self.workflow,
            #            'dashboard.wf.map.task_job' : self.task_map,
            "dashboard.xwf.start": self.workflowstate,
            "dashboard.xwf.end": self.workflowstate,
        }

        # Dicts for caching FK lookups
        self.wf_id_cache = {}
        self.root_wf_id_cache = {}

        # undocumented performance option
        self._perf = perf
        if self._perf:
            self._insert_time, self._insert_num = 0, 0
            self._start_time = time.time()

        # caches for batched events
        self._batch_cache = {
            "batch_events": [],
            "update_events": [],
            "host_map_events": [],
        }

    def process(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Get the BP dict from the controlling process and dispatch
        to the appropriate method per-event.
        """
        self.log.debug("process: %s", linedata)

        if not self._batch:
            self.check_connection()

        try:
            if self._perf:
                t = time.time()
                self.eventMap[linedata["event"]](linedata)
                self._insert_time += time.time() - t
                self._insert_num += 1
            else:
                self.eventMap[linedata["event"]](linedata)
        except KeyError:
            self.log.error('no handler for event type "%s" defined', linedata["event"])
        except exc.IntegrityError as e:
            # This is raised when an attempted insert violates the
            # schema (unique indexes, etc).
            self.log.error('Insert failed for event "%s" : %s', linedata["event"], e)
            self.session.rollback()
        except exc.OperationalError as e:
            self.log.error("Connection seemingly lost - attempting to refresh")
            self.session.rollback()
            self.check_connection()
            self.process(linedata)

        self.check_flush(increment=True)

    def linedataToObject(self, linedata, o):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        @type   o: instance of mapper class from stampede_dashboard_schema module.
        @param  o: Passed in by the appropriate event handler method.

        Takes the dict of BP linedata, assigns contents to the class o
        as attributes, and does any global type massaging like
        transforming dict strings to numeric types.
        """
        for k, v in linedata.items():
            if k == "level":
                continue

            # undot
            attr = k.replace(".", "_")

            attr_remap = {
                # workflow
                "xwf_id": "wf_uuid",
            }

            # remap attr names
            if attr in attr_remap:
                attr = attr_remap[attr]

            # sanitize argv input
            if attr == "argv":
                if v is not None:
                    v = v.replace("\\", "\\\\")
                    v = v.replace("'", "\\'")

            try:
                setattr(o, attr, v)
            except Exception:
                self.log.error("unable to process attribute %s with values: %s", k, v)

        # global type re-assignments
        if hasattr(o, "ts"):
            # make all timestamp values floats
            o.ts = float(o.ts)
        if hasattr(o, "restart_count") and o.restart_count is not None:
            o.restart_count = int(o.restart_count)
        return o

    #############################################
    # Methods to handle batching/flushing
    #############################################

    def hard_flush(self, batch_flush=True, retry=0):
        """
        @type   batch_flush: boolean
        @param  batch_flush: Defaults to true.  Is set to false
            when the batch commit hits and integrity error.

        Process queued inserts and flush/commit to the database.
        If the commit fails due to an integrity error, then method
        re-calls itself with setting batch_flush to False which
        causes each insert/object to be committed individually
        so all the "good" inserts can succeed.  This will increase
        the processing time of the batch with the bad data in it.
        """
        if not self._batch:
            return

        self.log.debug("Hard flush: batch_flush=%s", batch_flush)
        if retry == self.MAX_RETRIES + 1:
            # PM-1013 see if max retries is reached
            self.log.error(
                "Maximum number of retries reached for dashboard_loader.hard_flush() method %s"
                % self.MAX_RETRIES
            )
            raise RuntimeError(
                "Maximum number of retries reached for dashboard_loader.hard_flush() method %s"
                % self.MAX_RETRIES
            )

        retry = retry + 1
        self.check_connection()

        if self._perf:
            s = time.time()

        end_event = []

        for event in self._batch_cache["batch_events"]:
            if event.event == "dashboard.xwf.end":
                end_event.append(event)
            if batch_flush:
                self.session.add(event)
            else:
                self.individual_commit(event)

        for event in self._batch_cache["update_events"]:
            if batch_flush:
                self.session.merge(event)
            else:
                self.individual_commit(event, merge=True)

        try:
            self.session.commit()
        except exc.IntegrityError as e:
            self.log.error(
                "Integrity error on batch flush: %s - batch will need to be committed per-event which will take longer",
                e,
            )
            self.session.rollback()
        except exc.OperationalError as e:
            self.hard_flush(batch_flush=False, retry=retry)
            self.log.error(
                "Connection problem during commit: %s - reattempting batch", e
            )
            self.session.rollback()
            self.hard_flush(retry=retry)

        for host in self._batch_cache["host_map_events"]:
            self.map_host_to_job_instance(host)

        for ee in end_event:
            self.purgeCaches(ee)
        end_event = []

        # Clear all data structures here.
        for k in self._batch_cache.keys():
            self._batch_cache[k] = []

        self.session.commit()
        self.reset_flush_state()

        if self._perf:
            self.log.info("Hard flush duration: %s", (time.time() - s))

    #############################################
    # Methods to handle the various insert events
    #############################################
    def workflow(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a workflow insert event.
        """
        wf = self.linedataToObject(linedata, MasterWorkflow())
        self.log.debug("workflow: %s", wf)

        wf.timestamp = wf.ts

        is_root = True

        # for time being we don't track these. Karan
        #        if wf.root_xwf_id != wf.wf_uuid:
        #            is_root = False
        #            wf.root_wf_id = self.wf_uuid_to_id(wf.root_xwf_id)
        #
        #        if wf.parent_wf_id is not None:
        #            wf.parent_wf_id = self.wf_uuid_to_id(wf.parent_wf_id)

        # workflow inserts must be explicitly written to db whether
        # batching or not
        wf.commit_to_db(self.session)
        if is_root:
            wf.root_wf_id = self.wf_uuid_to_id(wf.root_xwf_id)
            wf.commit_to_db(self.session)
        if wf.root_wf_id is None:
            self.log.warn("Could not determine root_wf_id for event %s", wf)

    def workflowstate(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a workflowstate insert event.
        """
        wfs = self.linedataToObject(linedata, MasterWorkflowstate())
        self.log.debug("workflowstate: %s", wfs)

        state = {
            "dashboard.xwf.start": "WORKFLOW_STARTED",
            "dashboard.xwf.end": "WORKFLOW_TERMINATED",
        }

        wfs.wf_id = self.wf_uuid_to_id(wfs.wf_uuid)
        wfs.timestamp = wfs.ts
        wfs.state = state[wfs.event]

        if self._batch:
            self._batch_cache["batch_events"].append(wfs)
        else:
            wfs.commit_to_db(self.session)
            if wfs.event == "dashboard.xwf.end":
                self.purgeCaches(wfs)

    ####################################
    # DB helper/lookup/caching functions
    ####################################
    def wf_uuid_to_id(self, wf_uuid):
        """
        @type   wf_uuid: string
        @param  wf_uuid: wf_uuid string from BP logs

        Attempts to retrieve a workflow wf_id PK/FK from cache.  If
        not in cache, retrieve from st_workflow table in DB and cache.
        Cuts down on DB queries during insert processing.
        """
        if wf_uuid not in self.wf_id_cache:
            query = self.session.query(MasterWorkflow).filter(
                MasterWorkflow.wf_uuid == wf_uuid
            )
            try:
                self.wf_id_cache[wf_uuid] = query.one().wf_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error("Multiple wf_id results for wf_uuid %s : %s", wf_uuid, e)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error("No wf_id results for wf_uuid %s : %s", wf_uuid, e)
                return None

        return self.wf_id_cache[wf_uuid]

    def wf_uuid_to_root_id(self, wf_uuid):
        """
        @type   wf_uuid: string
        @param  wf_uuid: wf_uuid string from BP logs

        Attempts to retrieve a root workflow wf_id PK/FK from cache.  If
        not in cache, retrieve from st_workflow table in DB and cache.
        Cuts down on DB queries during insert processing.
        """
        if wf_uuid not in self.root_wf_id_cache:
            query = self.session.query(Workflow).filter(
                MasterWorkflow.wf_uuid == wf_uuid
            )
            try:
                self.root_wf_id_cache[wf_uuid] = query.one().root_wf_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error("Multiple wf_id results for wf_uuid %s : %s", wf_uuid, e)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error("No wf_id results for wf_uuid %s : %s", wf_uuid, e)
                return None

        return self.root_wf_id_cache[wf_uuid]

    def purgeCaches(self, wfs):
        """
        @type   wfs: class instance of stampede_schema.Workflowstate
        @param  wfs: Workflow state object from an end event.

        Purges information from the lookup caches after a workflow.end
        event has been recieved.
        """
        self.log.debug("Purging caches for: %s", wfs.wf_uuid)

        """
        for k,v in self.wf_id_cache.items():
            if k == wfs.wf_uuid:
                del self.wf_id_cache[k]

        for k,v in self.root_wf_id_cache.items():
            if k == wfs.wf_uuid:
                del self.root_wf_id_cache[k]
        """
        self.purgeCache(self.wf_id_cache, wfs.wf_uuid)
        self.purgeCache(self.root_wf_id_cache, wfs.wf_uuid)

    def purgeCache(self, cache, key):
        """
        Removes from a cache an entry matching a key id
        :param cache:
        :param key_id:
        :return:
        """

        if key in cache:
            del cache[key]

    ################
    # Cleanup, etc
    ################

    def finish(self):
        if self._batch:
            self.log.info("Executing final flush")
            self.hard_flush()
        self.disconnect()
        if self._perf:
            run_time = time.time() - self._start_time
            self.log.info(
                "Loader performance: insert_time=%s, insert_num=%s, "
                "total_time=%s, run_time_delta=%s, mean_time=%s",
                self._insert_time,
                self._insert_num,
                run_time,
                run_time - self._insert_time,
                self._insert_time / self._insert_num,
            )
