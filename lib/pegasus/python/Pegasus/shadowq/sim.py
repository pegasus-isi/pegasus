import logging
import time
from heapq import heappush, heappop

from Pegasus.shadowq.dag import JobState

PRE_SCRIPT_DELAY = 5.0
POST_SCRIPT_DELAY = 5.0
SCHEDULER_INTERVAL = 20.0

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
        log.info("%s" % (message))

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


# TODO Do we care about MAXPRE and MAXPOST?
# TODO Do we care about MAXJOBS?

class WorkflowEngine(Entity):
    def __init__(self, name, simulation, jobs, slots=1):
        Entity.__init__(self, name, simulation)
        self.jobs = jobs
        self.slots = slots
        self.queue = []
        self.runtime = 0.0
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
                delay = max(0, PRE_SCRIPT_DELAY - (time.time() - j.last_update))
                self.run_prescript(j, delay=delay)
            elif j.state == JobState.QUEUED:
                self.queue_job(j)
            elif j.state == JobState.RUNNING:
                # Compute time remaining for running job
                delay = max(0, j.runtime - (time.time() - j.last_update))
                self.run_job(j, delay)
            elif j.state == JobState.POSTSCRIPT:
                # Compute time remaining for post script
                delay = max(0, POST_SCRIPT_DELAY - (time.time() - j.last_update))
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

    def ready_job(self, job):
        # This job is ready, either run prescript or queue it
        if job.prescript:
            self.run_prescript(job)
        else:
            self.queue_job(job)

    def run_prescript(self, job, delay=PRE_SCRIPT_DELAY):
        job.state = JobState.PRESCRIPT
        self.send(self, 'prescript.finished', delay=delay, data=job)

    def queue_job(self, job):
        job.state = JobState.QUEUED
        # The heapq package uses min heaps, so we use negative priority
        heappush(self.queue, (-job.priority, job))

    def run_job(self, job, delay=None):
        # The job uses 1 slot
        self.slots -= 1

        job.state = JobState.RUNNING

        if delay is None:
            delay = job.runtime

        self.send(self, 'job.finished', delay=delay, data=job)

    def run_postscript(self, job, delay=POST_SCRIPT_DELAY):
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
            job.state = JobState.SUCCESSFUL
            self.queue_ready(job.children)

    def postscript_finished(self, job):
        self.log("post script finished for %s" % job.name)
        job.state = JobState.SUCCESSFUL
        self.queue_ready(job.children)

    def process(self, ev):
        if ev.tag == 'sim.start':
            self.schedule()
        elif ev.tag == 'prescript.finished':
            self.prescript_finished(ev.data)
        elif ev.tag == 'job.finished':
            self.job_finished(ev.data)
        elif ev.tag == 'postscript.finished':
            self.postscript_finished(ev.data)
        elif ev.tag == 'schedule':
            self.schedule()
        elif ev.tag == 'sim.stop':
            self.runtime = self.time()

