import logging
import time
from heapq import heappush, heappop

from Pegasus.shadowq.dag import JobState

PRE_SCRIPT_DELAY = 5.0
POST_SCRIPT_DELAY = 5.0
SCHEDULER_CYCLE_DELAY = 20.0
SCHEDULER_INTERVAL = 60.0
SCHEDULER_RESCHEDULE_DELAY = 0.0
DAGMAN_INITIAL_DELAY = 12.0

log = logging.getLogger(__name__)

class SimulationException(Exception):
    pass

class Event(object):
    __slots__ = ['to','fm','when','tag','data']

    def __init__(self, to, fm, when, tag, data):
        self.to = to
        self.fm = fm
        self.when = when
        self.tag = tag
        self.data = data

    def __str__(self):
        return "%s -> %s@%s %s: %s" % (self.to, self.fm, self.when, self.tag, self.data)

class Entity(object):
    def __init__(self, name, simulation):
        self.name = name
        self.simulation = simulation
        simulation.add(self)

    def process(self, ev):
        raise SimulationException("process() method not implemented")

    def send(self, to, tag, data=None, delay=0):
        when = self.simulation.time() + delay
        fm = self
        ev = Event(to, fm, when, tag, data)
        self.simulation.event(ev)

    def time(self):
        return self.simulation.time()

    def log(self, message):
        log.debug("%s@%0.3f %s" % (self.name, self.time(), message))

class Simulation(object):
    def __init__(self, start=0.0):
        self.start = start
        self.clock = start
        self.events = []
        self.entities = set()
        self.running = False

    def add(self, entity):
        self.entities.add(entity)

    def time(self):
        return self.clock

    def stop(self):
        self.log("Simulation stopped")
        self.running = False

    def log(self, message):
        log.debug("%s" % (message))

    def simulate(self, until=None):
        self.log("Simulation starting...")

        for entity in self.entities:
            self.event(Event(entity, self, self.clock, 'sim.start', None))

        self.running = True

        while self.running:
            if len(self.events) == 0:
                self.log("No more events")
                break

            if until is not None and self.events[0][0] > until:
                break

            when, ev = heappop(self.events)

            self.clock = when

            if not ev.to in self.entities:
                raise SimulationException("Unknown entity: %s", ev.to)

            ev.to.process(ev)

        if until is not None:
            self.clock = until

        self.stop = self.clock

        for entity in self.entities:
            ev = Event(entity, self, self.clock, 'sim.stop', None)
            entity.process(ev)

        self.log("Simulation finished")
        self.log("%d events are queued" % len(self.events))
        self.log("%d entities remain" % len(self.entities))

    def event(self, ev):
        if ev.when < self.clock:
            raise SimulationException("Event generated in the past")
        heappush(self.events, (ev.when, ev))

    def cancel(self, tag):
        newevents = []
        for when, ev in self.events:
            if ev.tag != tag:
                newevents.append((when, ev))
        self.events = newevents

    def find_next(self, tag):
        for when, ev in self.events:
            if ev.tag == tag:
                return when


# TODO Do we care about MAXPRE and MAXPOST?
# TODO Do we care about MAXJOBS?

class WorkflowEngine(Entity):
    def __init__(self, name, simulation, dag, slots=1):
        Entity.__init__(self, name, simulation)
        self.dag = dag
        self.jobs = dag.jobs
        self.slots = slots
        self.queue = []
        self.runtime = 0.0
        self.last_schedule = 0.0
        self.initialize_state()

    def initialize_state(self):
        # Set the initial state of the workflow
        for j in self.jobs.values():
            if j.state == JobState.UNREADY:
                # Nothing to do here until the parent finishes
                pass
            elif j.state == JobState.READY:
                self.ready_job(j)
            elif j.state == JobState.PRESCRIPT:
                # Compute time remaining for prescript
                # FIXME Some prescripts may run for a long time
                delay = max(0, PRE_SCRIPT_DELAY - (self.simulation.start - j.prescript_start))
                self.run_prescript(j, delay=delay)
            elif j.state == JobState.QUEUED:
                self.queue_job(j)
            elif j.state == JobState.RUNNING:
                # Compute time remaining for running job
                delay = max(0, j.runtime - (self.simulation.start - j.running_start))
                self.run_job(j, delay)
            elif j.state == JobState.POSTSCRIPT:
                # Compute time remaining for post script
                delay = max(0, POST_SCRIPT_DELAY - (self.simulation.start - j.postscript_start))
                self.run_postscript(j, delay)
            elif j.state == JobState.SUCCESSFUL:
                # Don't need to do anything
                pass
            elif j.state == JobState.FAILED:
                # If the job has some retries remaining, then we resubmit it
                # Otherwise it remains failed and none of its descendands run
                if j.failures <= j.retries:
                    # We assume that this job will succeed on retry
                    self.ready_job(j)
            else:
                raise SimulationException("Unknown job state: %s", j.state)

        # Cancel any initial schedules generated above
        self.simulation.cancel('schedule')

        # We know that the last schedule couldn't be older than the dag start
        self.last_schedule = self.dag.start

        # We set the last schedule based on the last time a job started running.
        # This is to avoid oscillations in the start/finish estimates. We know
        # running jobs represent a reschedule because jobs only start after
        # being scheduled onto a resource. In reality, there is probably a
        # slight delay (1-5 seconds) between scheduling and the start of a
        # job. That delay is larger in the case of condorio. Note that
        # running_start defaults to 0.
        for j in self.jobs.values():
            self.last_schedule = max(self.last_schedule, j.running_start)

        # Several scheduling cycles may have elapsed since the last one we
        # have evidence for
        while self.last_schedule + SCHEDULER_INTERVAL < self.time():
            self.last_schedule += SCHEDULER_INTERVAL

        log.debug("Last real schedule: %f", self.last_schedule)

        # Figure out the next schedule time
        if self.dag.start is None:
            # Workflow hasn't even started yet
            next_schedule = self.time() + DAGMAN_INITIAL_DELAY
        elif self.time() - self.dag.start < DAGMAN_INITIAL_DELAY:
            # We are at the beginning of the workflow
            next_schedule = self.dag.start + DAGMAN_INITIAL_DELAY
        else:
            # The next one should be one SCHEDULER_INTERVAL from the last
            next_schedule = self.last_schedule + SCHEDULER_INTERVAL

            # But if there are any queued jobs, then the first job that was queued
            # after the last scheduling interval will determine the next schedule
            for j in self.jobs.values():
                # If a job was queued after the last_schedule, then the next
                # schedule will be after a minimum cycle delay. The 5.0 is just
                # a fudge factor based on DAGMan's polling interval.
                if j.state == JobState.QUEUED and j.queue_start >= (self.last_schedule - 5.0):
                    log.debug("Setting next_schedule based on queued job: %s", j.name)
                    next_schedule = min(next_schedule, j.queue_start + SCHEDULER_CYCLE_DELAY)

        # If the next schedule time is in the past, schedule now, otherwise schedule
        # at the right time
        delay_until_next_schedule = next_schedule - self.time()
        if delay_until_next_schedule < 0:
            delay_until_next_schedule = 0

        log.debug("Delay until next simulated schedule: %f", delay_until_next_schedule)

        self.send(self, 'schedule', delay=delay_until_next_schedule)

    def ready_job(self, job):
        # This job is ready, either run prescript or queue it
        if job.prescript:
            self.run_prescript(job)
        else:
            self.queue_job(job)

    def run_prescript(self, job, delay=PRE_SCRIPT_DELAY):
        log.debug("%s %s %s", self.time(), job.name, "PRESCRIPT")

        job.state = JobState.PRESCRIPT
        self.send(self, 'prescript.finished', delay=delay, data=job)

    def queue_job(self, job):
        log.debug("%s %s %s", self.time(), job.name, "QUEUED")

        job.state = JobState.QUEUED
        # The heapq package uses min heaps, so we use negative priority
        # We use the job sequence number in the DAG file to break ties because
        # that is the order in which DAGMan submits the jobs to the queue.
        heappush(self.queue, ((-job.priority, job.sequence), job))

        # If the last scheduler cycle was recent, then schedule now
        log.debug("last_schedule: %s", self.last_schedule)
        next_schedule = self.simulation.find_next('schedule')
        if self.last_schedule + SCHEDULER_CYCLE_DELAY <= self.time():
            self.simulation.cancel('schedule') # Cancel future schedule events
            self.send(self, 'schedule', delay=0.0)
        elif next_schedule and (next_schedule - self.time()) >= SCHEDULER_CYCLE_DELAY:
            self.simulation.cancel('scheule')
            self.send(self, 'schedule', delay=SCHEDULER_CYCLE_DELAY)

    def run_job(self, job, delay=None):
        log.debug("%s %s %s", self.time(), job.name, "RUNNING")

        # The job uses 1 slot
        self.slots -= 1

        job.state = JobState.RUNNING

        if delay is None:
            delay = job.runtime

        self.send(self, 'job.finished', delay=delay, data=job)

    def run_postscript(self, job, delay=POST_SCRIPT_DELAY):
        log.debug("%s %s %s", self.time(), job.name, "POSTSCRIPT")

        job.state = JobState.POSTSCRIPT
        self.send(self, 'postscript.finished', delay=delay, data=job)

    def queue_ready(self, jobs):
        for c in jobs:
            if c.state != JobState.UNREADY:
                continue
            ready = True
            for p in c.parents:
                if p.state != JobState.SUCCESSFUL:
                    ready = False
            if ready:
                self.ready_job(c)

        if self.workflow_finished():
            self.simulation.stop()

    def workflow_finished(self):
        # The workflow is finished if all the jobs are blocked or finished
        finished = True
        for j in self.jobs.values():
            if j.state not in (JobState.UNREADY, JobState.SUCCESSFUL, JobState.FAILED):
                finished = False
                break
        return finished

    def schedule(self):
        self.last_schedule = self.time()

        self.log("Scheduling %d jobs on %d slots..." % (len(self.queue), self.slots))
        while self.slots > 0 and len(self.queue) > 0:
            _, job = heappop(self.queue)
            self.run_job(job)

        self.send(self, 'schedule', delay=SCHEDULER_INTERVAL)

    def prescript_finished(self, job):
        self.log("pre script finished for %s" % job.name)
        self.queue_job(job)

    def job_finished(self, job):
        self.log("job %s finished" % job.name)

        # One slot is free
        self.slots += 1

        if job.postscript:
            # Job has post script, so run it
            self.run_postscript(job)
        else:
            # Job has no post script, just mark it successful
            log.info("%s %s %s", self.time(), job.name, "SUCCESSFUL")
            job.state = JobState.SUCCESSFUL
            self.queue_ready(job.children)

    def postscript_finished(self, job):
        log.debug("%s %s %s", self.time(), job.name, "SUCCESSFUL")
        self.log("post script finished for %s" % job.name)
        job.state = JobState.SUCCESSFUL
        self.queue_ready(job.children)

    def process(self, ev):
        if ev.tag == 'sim.start':
            pass
        elif ev.tag == 'prescript.finished':
            self.prescript_finished(ev.data)
        elif ev.tag == 'job.finished':
            self.job_finished(ev.data)
        elif ev.tag == 'postscript.finished':
            self.postscript_finished(ev.data)
        elif ev.tag == 'schedule':
            self.schedule()
        elif ev.tag == 'sim.stop':
            self.runtime = self.time() - self.simulation.start

