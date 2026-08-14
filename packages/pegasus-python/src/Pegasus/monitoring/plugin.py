"""
Plugin system for pegasus-monitord.

Third-party packages can register a :class:`MonitordEventPlugin` under the
``pegasus.monitord.plugins`` entry-point group to receive every stampede
workflow event in a dedicated background thread, without forking monitord or
scraping the stampede database after the fact.

Discovery is opt-in: a plugin is only run when it is both (a) installed and
registered under the entry-point group, and (b) explicitly enabled via the
property ``pegasus.monitord.plugins.<name>.enabled = true``.

The host side (the sink that feeds these plugins from monitord's event stream)
lives in :mod:`Pegasus.monitoring.event_output` as ``PluginHostEventSink``.
"""

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

import asyncio
import collections
import copy
import inspect
import logging
import queue
import time
import traceback
from threading import Event, Lock, Thread

from Pegasus.tools import utils

log = logging.getLogger(__name__)

# Entry-point group third-party packages register their plugins under.
MONITORD_PLUGIN_ENTRY_POINT_GROUP = "pegasus.monitord.plugins"

# Property namespace all plugin configuration lives under.
MONITORD_PLUGIN_PROPERTY_PREFIX = "pegasus.monitord.plugins."

_ENABLED_SUFFIX = ".enabled"

# Per-plugin defaults (overridable via pegasus.monitord.plugins.<name>.*).
DEFAULT_QUEUE_SIZE = 10000
DEFAULT_JOIN_TIMEOUT = 10.0
DEFAULT_TICK_INTERVAL = 0.0  # 0 = no ticks; the worker blocks exactly as before
DEFAULT_EVENT_TIMEOUT = 0.0  # 0 = no per-event bound (async plugins included)

# Queue-overflow policies (pegasus.monitord.plugins.<name>.overflow_policy).
OVERFLOW_DROP_NEWEST = "drop-newest"
OVERFLOW_DROP_OLDEST = "drop-oldest"
DEFAULT_OVERFLOW_POLICY = OVERFLOW_DROP_NEWEST

# Distinct from None, which could in principle be a queued value.
_NOTHING = object()

# Payload values of these EXACT types are immutable, so sharing them by
# reference between the producer and every plugin is safe and skips
# deepcopy's per-object machinery (deepcopy would return them by identity
# anyway -- this is a cost cut, not a semantic change). Exact-type match
# only: a subclass may carry mutable state, so it takes the deepcopy path.
_IMMUTABLE_SCALAR_TYPES = frozenset({str, int, float, bool, bytes, type(None)})


def _snapshot_payload(kw):
    """
    Per-plugin snapshot of an event payload: a new outer dict (the producer
    reuses and rebinds keys in the original after dispatch), sharing
    immutable scalar values by reference and deep-copying everything else
    (nested mutables in composite events must not be aliased across
    threads). Stampede payloads are overwhelmingly flat scalar dicts, so
    this avoids most of a blanket ``copy.deepcopy(kw)``'s cost.
    """
    # Reuse one deepcopy memo for the whole payload so aliases and cycles
    # spanning multiple top-level values retain the same object-graph shape
    # as ``copy.deepcopy(kw)``. Seeding the memo with the new outer dict also
    # preserves a nested reference back to the payload itself.
    snapshot = {}
    memo = {id(kw): snapshot}
    for key, val in kw.items():
        snapshot_key = (
            key if type(key) in _IMMUTABLE_SCALAR_TYPES else copy.deepcopy(key, memo)
        )
        snapshot[snapshot_key] = (
            val if type(val) in _IMMUTABLE_SCALAR_TYPES else copy.deepcopy(val, memo)
        )
    return snapshot


_WorkerConfig = collections.namedtuple(
    "_WorkerConfig",
    "queue_size start_timeout join_timeout tick_interval events overflow_policy"
    " event_timeout",
)


def _is_coroutine_callable(fn):
    """
    True when ``fn`` is declared ``async def``. Degrades to False (the sync
    dispatch path) on exotic callables that ``inspect`` cannot analyze,
    mirroring the defensive signature sniffing in ``_start_kwargs``.
    """
    try:
        return inspect.iscoroutinefunction(fn)
    except (TypeError, ValueError):
        return False


def enabled_plugin_names(props):
    """
    Return the set of plugin names with a truthy
    ``pegasus.monitord.plugins.<name>.enabled`` property.

    The name is everything between the namespace prefix and the *final*
    ``.enabled`` suffix -- the exact inverse of the per-name lookup in
    :meth:`MonitordPluginManager._is_enabled` -- so an (unconventional)
    dotted entry-point name still round-trips. A bare
    ``pegasus.monitord.plugins.enabled`` key names no plugin and is ignored.
    """
    if props is None:
        return set()
    names = set()
    subset = props.propertyset(MONITORD_PLUGIN_PROPERTY_PREFIX, True)
    for key, val in subset.items():
        if not key.endswith(_ENABLED_SUFFIX):
            continue
        name = key[: -len(_ENABLED_SUFFIX)]
        if name and utils.make_boolean(val):
            names.add(name)
    return names


class MonitordEventPlugin:
    """
    Base class for a pegasus-monitord event plugin.

    Subclasses override the hooks they care about; all three default to
    no-ops. A plugin is discovered via the ``pegasus.monitord.plugins``
    entry-point group and run only when explicitly enabled in properties.

    Threading contract:

    * :meth:`start` is called once before events flow, in a bounded helper
      thread during monitord startup. :meth:`stop` is called once after the
      event worker exits, also in a bounded helper thread. When several
      plugins are enabled, different plugins' ``start()`` (and ``stop()``)
      hooks may run concurrently with one another; each plugin's own
      lifecycle stays strictly ordered.
    * :meth:`handle_event` is called once per event on this plugin's *own*
      dedicated background thread. Events are delivered to a single plugin in
      order, but different plugins (and monitord's own database writer) run
      concurrently -- do not assume an event has been committed to the stampede
      database by the time you observe it.

    A single monitord daemon may process more than one workflow (e.g. a root
    workflow and its sub-workflows). Plugins that care about per-workflow state
    must demultiplex on the ``xwf__id`` / ``root__xwf__id`` keys, which are
    present on every event payload.
    """

    #: Optional event filter: ``None`` (default) delivers every event; a
    #: string or tuple of strings is matched as *prefixes* against the
    #: fully-qualified event name (e.g. ``"stampede.job_inst."``). An empty
    #: tuple delivers no events (tick-only plugins). Operators can override
    #: it with the ``pegasus.monitord.plugins.<name>.events`` property.
    event_filter = None

    def start(self, props=None, restart=False):
        """
        Called once, before any events flow.

        :param props: the full Pegasus properties object
            (:class:`Pegasus.tools.properties.Properties`). Read plugin config
            from ``pegasus.monitord.plugins.<name>.*`` keys, e.g.::

                cfg = props.propertyset(
                    "pegasus.monitord.plugins.myplugin.", remove=True
                )

        :param restart: ``True`` when monitord is re-emitting the entire
            event stream from the beginning of ``dagman.out`` -- either a
            user-requested replay (``pegasus-monitord -r``) or monitord's own
            recovery after an unclean shutdown. Every event a previous run
            may already have delivered to this plugin will be delivered
            again; truncate or deduplicate durable output the way monitord's
            own sinks do (the stampede DB rows are purged, ``jobstate.log``
            is rotated, the file sink truncates instead of appending).
            ``False`` on a normal first run.

        Backward compatibility: overrides may keep the historical
        one-argument signature ``start(self, props=None)``; the host passes
        ``restart`` only when the override names it or accepts ``**kwargs``.
        Accepting ``**kwargs`` is recommended so future context keywords do
        not require another signature change.

        If this hook does not return within ``start_timeout``, Pegasus skips
        the plugin and continues startup. Python cannot forcibly kill the
        helper thread, so Pegasus attempts bounded ``stop()`` cleanup if a
        timed-out ``start()`` returns later.
        """

    def handle_event(self, event, kw):
        """
        Called in this plugin's dedicated background thread for each event.

        :param event: the fully-qualified stampede event name, e.g.
            ``"stampede.job_inst.main.end"``.
        :param kw: the event payload dict. Keys use ``__`` as the separator
            (e.g. ``xwf__id``, ``job__id``); the payload is passed through
            unmodified, exactly as monitord produced it.

        May be declared ``async def``. An async handler runs on a private
        asyncio event loop owned by this plugin's worker thread (reach it
        with :func:`asyncio.get_running_loop`); events are still delivered
        strictly in order, one at a time, and :meth:`tick` still never runs
        concurrently. When ``pegasus.monitord.plugins.<name>.event_timeout``
        is set to a positive number of seconds, a handler exceeding it is
        **cancelled** and the worker moves on to the next event -- the one
        recovery a blocked sync handler can never get. Cancellation is
        *cooperative*: :class:`asyncio.CancelledError` is raised at the
        coroutine's next ``await``, so code that blocks synchronously inside
        a coroutine (e.g. ``requests.get``) starves the loop and wedges the
        worker exactly like a blocking sync handler. Write handlers
        cancellation-safe: cleanup belongs in ``finally``, which runs on the
        worker thread.
        """

    def tick(self):
        """
        Called periodically on this plugin's dedicated background thread (the
        same thread as :meth:`handle_event`, so the two never run concurrently
        and shared state needs no locking).

        Opt-in: ticks fire only when
        ``pegasus.monitord.plugins.<name>.tick_interval`` is set to a positive
        number of seconds. The cadence is *at most every interval*, not exact:
        a tick fires when the event queue has been idle for the interval, or
        immediately after an event when the interval has elapsed since the
        last tick. Use it for wall-clock work that must not depend on event
        flow (e.g. polling an external system while the workflow is quiet).

        Exceptions are caught and logged exactly like :meth:`handle_event`
        exceptions; a failing tick never kills the worker. ``tick()`` is never
        called after the shutdown sentinel has been drained, so it cannot race
        :meth:`stop`.

        May be declared ``async def``; it then runs on the same private
        event loop as an async :meth:`handle_event`, under the same
        ``event_timeout`` bound, and the two still never run concurrently.
        """

    def stop(self):
        """
        Called once after all events have been processed and this plugin's
        background thread has been joined. If the worker does not exit within
        ``join_timeout``, Pegasus skips ``stop()`` rather than racing cleanup
        against a still-running ``handle_event()``. A synchronous ``stop()``
        that does not return within ``join_timeout`` is abandoned; async
        cancellation behavior is described below.

        May be declared ``async def``: it is then driven on a throwaway
        event loop. After ``join_timeout`` the coroutine is **cancelled**;
        Pegasus waits up to one additional ``join_timeout`` for cooperative
        cancellation cleanup before abandoning the helper thread.
        """


class _PluginWorker:
    """
    Runs one plugin's :meth:`~MonitordEventPlugin.handle_event` on a dedicated
    daemon thread fed by a bounded queue.

    The design mirrors the proven async pattern in
    ``event_output.AMQPEventSink`` (queue + daemon thread + drain-and-join on
    close), with two deliberate hardening changes for untrusted third-party
    code: the queue is bounded with a drop-on-overflow policy so a stalled
    plugin cannot grow monitord's memory unbounded, and the shutdown join is
    bounded by a timeout so a wedged plugin cannot hang monitord's exit.
    """

    _SENTINEL = object()

    def __init__(
        self,
        name,
        plugin,
        queue_size=DEFAULT_QUEUE_SIZE,
        join_timeout=DEFAULT_JOIN_TIMEOUT,
        tick_interval=DEFAULT_TICK_INTERVAL,
        event_filter=None,
        overflow_policy=DEFAULT_OVERFLOW_POLICY,
        event_timeout=DEFAULT_EVENT_TIMEOUT,
    ):
        self._name = name
        self._plugin = plugin
        self._log = logging.getLogger(f"{__name__}._PluginWorker.{name}")
        self._join_timeout = join_timeout
        self._tick_interval = tick_interval
        self._event_filter = event_filter
        self._overflow_policy = overflow_policy
        self._event_timeout = event_timeout
        # ``async def`` hooks are detected once here; async plugins get a
        # private asyncio event loop owned by the worker thread (see _run)
        self._handle_is_async = _is_coroutine_callable(plugin.handle_event)
        self._tick_is_async = _is_coroutine_callable(plugin.tick)
        self._loop = None
        # queue_size <= 0 means unbounded (matches queue.Queue default)
        maxsize = queue_size if queue_size and queue_size > 0 else 0
        self._queue = queue.Queue(maxsize=maxsize)
        self._dropped = 0
        self._filtered = 0
        self._timed_out = 0
        self._warned_sync_coroutine = False
        self._thread = Thread(
            target=self._run, name=f"monitord-plugin-{name}", daemon=True
        )

    def start(self):
        self._thread.start()

    def _record_drop(self):
        """Count one lost event; True when this drop should be logged
        (the 1st and every 1000th, matching the established cadence)."""
        self._dropped += 1
        return self._dropped == 1 or self._dropped % 1000 == 0

    def submit(self, event, kw):
        """
        Enqueue an event for the plugin thread. Never blocks and never raises:
        a full queue drops an event (counted), and a dead worker is a no-op.
        This guarantees monitord's parse loop is never stalled by a plugin.

        Events rejected by the plugin's event filter are skipped *before* the
        payload snapshot below -- that is the entire point of filtering; they
        are counted separately and are not drops.

        The payload is snapshotted with :func:`_snapshot_payload` before it
        is queued. The worker thread reads the payload asynchronously, while
        monitord's main thread keeps -- and in places reuses/mutates -- the
        original dict (e.g. the per-LFN ``rc.meta`` loop in ``workflow.py``
        overwrites ``key``/``value`` and re-sends the same dict; ``wf.plan``
        adds ``db_url`` after the event is dispatched). A per-worker snapshot
        gives each plugin its own isolated, stable payload and removes that
        cross-thread data race: the new outer dict decouples it from the
        producer's key rebinding, immutable scalar values are shared by
        reference, and everything else -- including nested mutable values in
        composite events -- is deep-copied.

        On overflow, which event is lost depends on ``overflow_policy``:
        ``drop-newest`` (default) drops the event being submitted;
        ``drop-oldest`` evicts the oldest queued event so a live-monitoring
        plugin keeps the freshest state. Either way exactly one event is
        lost per overflow.
        """
        flt = self._event_filter
        if flt is not None and not event.startswith(flt):
            self._filtered += 1
            return
        if not self._thread.is_alive():
            return
        try:
            payload = _snapshot_payload(kw)
        except Exception:
            if self._record_drop():
                self._log.error(
                    "plugin %r could not snapshot event %s; dropped %d event(s) so far\n%s",
                    self._name,
                    event,
                    self._dropped,
                    traceback.format_exc(),
                )
            return
        item = (event, payload)
        try:
            self._queue.put_nowait(item)
            return
        except queue.Full:
            pass
        if self._overflow_policy == OVERFLOW_DROP_OLDEST:
            # Single-producer invariant: only monitord's main thread ever
            # put()s here (dispatch via the sink's send, and the shutdown
            # sentinel in close() strictly after the last send). The worker
            # only get()s. So the slot freed below cannot be stolen -- one
            # bounded retry, never a loop, never a block.
            evicted = _NOTHING
            try:
                evicted = self._queue.get_nowait()
            except queue.Empty:
                pass  # worker drained concurrently; a slot is free anyway
            else:
                self._queue.task_done()  # keep unfinished-task count balanced
            if evicted is self._SENTINEL:
                # Defensive only: unreachable under the invariant above. The
                # shutdown sentinel is sacred -- requeue it, drop the NEW
                # event instead (falls through to the accounting below).
                try:
                    self._queue.put_nowait(evicted)
                except queue.Full:  # pragma: no cover - single producer
                    self._log.error(
                        "plugin %r shutdown sentinel lost under overflow",
                        self._name,
                    )
            else:
                try:
                    self._queue.put_nowait(item)
                except queue.Full:  # pragma: no cover - single producer
                    pass  # fall through: count the new event as dropped
                else:
                    if evicted is _NOTHING:
                        return  # drain freed a slot; nothing was lost
                    if self._record_drop():
                        self._log.warning(
                            "plugin %r queue full; dropped oldest event(s), %d so far",
                            self._name,
                            self._dropped,
                        )
                    return
        if self._record_drop():
            self._log.warning(
                "plugin %r queue full; dropped %d event(s) so far",
                self._name,
                self._dropped,
            )

    def _handle(self, event, kw):
        try:
            if self._handle_is_async:
                self._run_coroutine(
                    self._plugin.handle_event(event, kw), f"handle_event({event})"
                )
            else:
                ret = self._plugin.handle_event(event, kw)
                if inspect.iscoroutine(ret):
                    self._warn_sync_returned_coroutine("handle_event", ret)
        except asyncio.CancelledError:
            # CancelledError inherits BaseException, not Exception. A plugin
            # may legitimately receive it from an awaited child operation;
            # letting it escape would silently kill this worker and strand
            # every later event in the queue.
            self._log.error(
                "plugin %r handle_event(%s) was cancelled; continuing",
                self._name,
                event,
            )
        except Exception:
            # A misbehaving plugin must never kill its own thread.
            self._log.error(
                "plugin %r handle_event(%s) raised:\n%s",
                self._name,
                event,
                traceback.format_exc(),
            )

    def _tick(self):
        try:
            if self._tick_is_async:
                self._run_coroutine(self._plugin.tick(), "tick()")
            else:
                ret = self._plugin.tick()
                if inspect.iscoroutine(ret):
                    self._warn_sync_returned_coroutine("tick", ret)
        except asyncio.CancelledError:
            self._log.error(
                "plugin %r tick() was cancelled; continuing",
                self._name,
            )
        except Exception:
            # Same isolation contract as handle_event: a failing tick is
            # logged, never fatal to the worker.
            self._log.error(
                "plugin %r tick() raised:\n%s",
                self._name,
                traceback.format_exc(),
            )

    def _run_coroutine(self, coro, what):
        """
        Drive one plugin coroutine to completion on this worker's private
        event loop (run_until_complete per event: per-plugin FIFO and the
        no-concurrent-tick guarantee hold exactly as for sync plugins).
        With a positive ``event_timeout``, a coroutine that exceeds it is
        *cancelled* -- the worker survives and moves on to the next queued
        item, instead of wedging forever the way a blocked sync handler
        does. Cancellation is cooperative: it lands at the coroutine's next
        ``await``, so synchronously-blocking code inside a coroutine still
        wedges.
        """
        timeout = self._event_timeout
        if not timeout or timeout <= 0:
            self._loop.run_until_complete(coro)
            return
        try:
            self._loop.run_until_complete(asyncio.wait_for(coro, timeout))
        except asyncio.TimeoutError:
            # wait_for has already cancelled the coroutine
            self._timed_out += 1
            self._log.error(
                "plugin %r %s did not complete within %.1fs; cancelled "
                "(%d cancelled so far)",
                self._name,
                what,
                timeout,
                self._timed_out,
            )

    def _warn_sync_returned_coroutine(self, hook, coro):
        """A sync-declared hook returned a coroutine object: the plugin
        author forgot ``async def``. Log once per worker and close the
        coroutine so it neither runs nor warns about never being awaited."""
        coro.close()
        if self._warned_sync_coroutine:
            return
        self._warned_sync_coroutine = True
        self._log.error(
            "plugin %r %s() returned a coroutine but is not declared "
            "'async def'; it will never run -- declare the hook async",
            self._name,
            hook,
        )

    def _run(self):
        if self._handle_is_async or self._tick_is_async:
            # One private event loop for this worker thread's whole
            # lifetime -- never a per-event asyncio.run(), which would tear
            # the loop machinery down and up for every event. Handlers use
            # asyncio.get_running_loop() to reach it.
            self._loop = asyncio.new_event_loop()
        try:
            self._consume()
        finally:
            if self._loop is not None:
                self._loop.close()

    def _consume(self):
        interval = self._tick_interval
        if not interval or interval <= 0:
            # No ticks configured: block exactly as before (zero overhead).
            while True:
                item = self._queue.get()
                try:
                    if item is self._SENTINEL:
                        return
                    event, kw = item
                    self._handle(event, kw)
                finally:
                    self._queue.task_done()

        # Ticking variant. The first tick fires only after one full idle
        # interval; thereafter at most every interval, from whichever comes
        # first: the queue going idle, or an event completing past the mark.
        last_tick = time.monotonic()
        while True:
            timeout = max(0.0, interval - (time.monotonic() - last_tick))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                # Idle path: nothing was dequeued, so no task_done().
                self._tick()
                last_tick = time.monotonic()
                continue
            try:
                if item is self._SENTINEL:
                    # FIFO drain guarantee: both tick sites are unreachable
                    # from here, so tick() can never follow the sentinel.
                    return
                event, kw = item
                self._handle(event, kw)
            finally:
                self._queue.task_done()
            # Starvation guard: under continuous event flow get() never times
            # out, so tick here once the interval has lapsed.
            if time.monotonic() - last_tick >= interval:
                self._tick()
                last_tick = time.monotonic()

    def close(self):
        """
        Drain queued events, then stop and join the worker thread (bounded by
        ``join_timeout``). Returns once the thread has exited or the timeout
        has elapsed. Logs a final total if any events were dropped.
        """
        # _dropped/_filtered are written only by submit() (monitord's main
        # thread, via the sink's send) and read only here. close() may run on
        # a per-plugin teardown helper thread, but every submit() has ceased
        # before that helper is spawned and Thread.start() publishes all
        # prior writes to it -- so no lock is needed and the totals are
        # final.
        if self._dropped:
            self._log.warning(
                "plugin %r dropped %d event(s) in total "
                "(queue overflow or payload snapshot failure)",
                self._name,
                self._dropped,
            )
        if self._filtered:
            self._log.info(
                "plugin %r filtered %d event(s) by its event filter",
                self._name,
                self._filtered,
            )
        # _timed_out is written on the worker thread (unlike the two above);
        # reading it here is safe only after the join below, so report it in
        # a second pass at the end of this method.
        if not self._thread.is_alive():
            return True
        try:
            # FIFO sentinel: the worker drains everything ahead of it first.
            self._queue.put(self._SENTINEL, timeout=self._join_timeout)
        except queue.Full:
            self._log.warning(
                "plugin %r queue full at shutdown; some events may be unprocessed",
                self._name,
            )
        self._thread.join(timeout=self._join_timeout)
        if self._thread.is_alive():
            self._log.warning(
                "plugin %r worker did not exit within %.1fs; abandoning it",
                self._name,
                self._join_timeout,
            )
            return False
        # joined: the worker's _timed_out writes happened-before this read
        if self._timed_out:
            self._log.warning(
                "plugin %r had %d handler call(s) cancelled on event_timeout",
                self._name,
                self._timed_out,
            )
        return True


class MonitordPluginManager:
    """
    Daemon-level owner of the enabled monitord event plugins.

    Discovers plugins registered under ``pegasus.monitord.plugins``, filters to
    those explicitly enabled in properties, instantiates and ``start()``s each
    once, gives each its own :class:`_PluginWorker` (thread + queue), and tears
    them all down on shutdown.
    """

    def __init__(self, props=None, restart=False):
        self._props = props
        self._restart = bool(restart)
        self._log = logging.getLogger(f"{__name__}.MonitordPluginManager")
        # list of (name, plugin, worker)
        self._workers = []

    def discover_and_start(self):
        """
        Discover, start, and spin up a worker thread for each enabled plugin.
        Returns the number of plugins started. A plugin that fails to load,
        instantiate, or ``start()`` is logged and skipped -- it never aborts
        the others or monitord itself. A name that is enabled in properties
        but not registered under the entry-point group is logged as a warning
        and skipped.

        When more than one plugin is enabled, the per-plugin startup sequences
        run concurrently (one helper thread each), so N slow ``start()``s cost
        one ``start_timeout``, not N. Survivors are recorded in discovery
        (entry-point) order regardless of completion order.
        """
        entry_points = self._discover_entry_points()
        self._warn_unmatched_enabled_names(entry_points)
        enabled = list(self._iter_enabled_entry_points(entry_points))
        if len(enabled) <= 1:
            results = [
                self._start_one(name, entry_point) for name, entry_point in enabled
            ]
        else:
            results = self._fan_out(
                "startup",
                [(name, (name, entry_point)) for name, entry_point in enabled],
                lambda name, entry_point: self._start_one(name, entry_point),
            )
        for item in results:
            if item is not None:
                self._workers.append(item)
        return len(self._workers)

    def _start_one(self, name, entry_point):
        """
        Run one plugin's complete bootstrap under ``start_timeout``: load the
        entry point, construct the instance, call ``start()``, compile its
        filter, and start its worker. Returns the ``(name, plugin, worker)``
        triple, or ``None`` when the plugin is skipped.

        Python cannot kill the helper thread. If a timed-out bootstrap returns
        later, it observes ``timed_out`` and performs bounded cleanup without
        publishing the plugin to the manager.
        """
        try:
            cfg = self._worker_config(name)
        except Exception:
            self._log.error(
                "invalid configuration for plugin %r; skipping\n%s",
                name,
                traceback.format_exc(),
            )
            return None

        done = Event()
        timed_out = Event()
        cleanup_lock = Lock()
        cleanup_started = []
        result = {
            "plugin": None,
            "worker": None,
            "triple": None,
            "traceback": None,
        }

        def cleanup_after_timeout():
            with cleanup_lock:
                if cleanup_started:
                    return
                cleanup_started.append(True)
            if result["traceback"]:
                self._log.error(
                    "plugin %r bootstrap failed after startup timeout\n%s",
                    name,
                    result["traceback"],
                )
            elif result["plugin"] is not None:
                self._log.warning(
                    "plugin %r bootstrap returned after startup timeout; "
                    "running stop() cleanup",
                    name,
                )
            self._cleanup_failed_start(
                name,
                result["plugin"],
                result["worker"],
                cfg.join_timeout,
            )

        def bootstrap_target():
            try:
                cls = entry_point.load()
                if timed_out.is_set():
                    return
                plugin = cls()
                result["plugin"] = plugin
                if timed_out.is_set():
                    return
                plugin.start(self._props, **self._start_kwargs(plugin))
                if timed_out.is_set():
                    return
                # Read from the instance after start() so plugins may derive
                # their filter dynamically (e.g. from props) in start().
                event_filter = self._compile_event_filter(name, cfg.events, plugin)
                if timed_out.is_set():
                    return
                worker = _PluginWorker(
                    name,
                    plugin,
                    queue_size=cfg.queue_size,
                    join_timeout=cfg.join_timeout,
                    tick_interval=cfg.tick_interval,
                    event_filter=event_filter,
                    overflow_policy=cfg.overflow_policy,
                    event_timeout=cfg.event_timeout,
                )
                result["worker"] = worker
                if timed_out.is_set():
                    return
                worker.start()
                result["triple"] = (name, plugin, worker)
            except Exception:
                result["traceback"] = traceback.format_exc()
            finally:
                done.set()
                if timed_out.is_set():
                    cleanup_after_timeout()

        bootstrap_thread = Thread(
            target=bootstrap_target,
            name=f"monitord-plugin-{name}-start",
            daemon=True,
        )
        bootstrap_thread.start()
        bootstrap_thread.join(timeout=cfg.start_timeout)
        if bootstrap_thread.is_alive():
            timed_out.set()
            self._log.warning(
                "plugin %r bootstrap did not complete within %.1fs; skipping it",
                name,
                cfg.start_timeout,
            )
            if done.is_set():
                cleanup_after_timeout()
            return None
        if result["traceback"]:
            self._log.error(
                "failed to start plugin %r; skipping\n%s",
                name,
                result["traceback"],
            )
            self._cleanup_failed_start(
                name,
                result["plugin"],
                result["worker"],
                cfg.join_timeout,
            )
            return None
        if result["triple"] is not None:
            self._log.info("started monitord event plugin %r", name)
        return result["triple"]

    def _fan_out(self, stage, named_args, fn):
        """
        Run ``fn(*args)`` once per ``(name, args)`` entry, each on its own
        daemon helper thread, and return the results in input order.

        Plain threads rather than a ``concurrent.futures`` pool: ``stop_all``
        runs from monitord's atexit handler, where importing/creating an
        executor has interpreter-shutdown interplay that bare ``Thread`` +
        ``join`` (already used in this path today) does not. The joins carry
        no timeout because every operation inside ``fn`` is itself bounded.
        """
        results = [None] * len(named_args)

        def _task(idx, args):
            results[idx] = fn(*args)

        threads = [
            Thread(
                target=_task,
                args=(idx, args),
                name=f"monitord-plugin-{name}-{stage}",
                daemon=True,
            )
            for idx, (name, args) in enumerate(named_args)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def dispatch(self, event, kw):
        """
        Hand an event to every running plugin's worker. Non-blocking.
        """
        for _name, _plugin, worker in self._workers:
            worker.submit(event, kw)

    def stop_all(self):
        """
        Drain and join every plugin worker, then call each plugin's ``stop()``
        in a bounded helper thread. Per-plugin ordering matches the contract:
        ``stop()`` starts only after that plugin's event thread has been
        joined. Different plugins tear down concurrently (one helper thread
        each), so shutdown is bounded by the slowest plugin, not the sum.
        """
        workers = self._workers
        if len(workers) <= 1:
            for name, plugin, worker in workers:
                self._stop_one(name, plugin, worker)
        else:
            self._fan_out(
                "teardown",
                [(entry[0], entry) for entry in workers],
                self._stop_one,
            )
        self._workers = []

    def _stop_one(self, name, plugin, worker):
        """
        Tear one plugin down: drain and join its worker, then run ``stop()``
        -- skipped when the worker misses its join timeout, so cleanup never
        races a still-running ``handle_event``.
        """
        worker_exited = False
        try:
            worker_exited = worker.close()
        except Exception:
            self._log.error(
                "error closing worker for plugin %r\n%s",
                name,
                traceback.format_exc(),
            )
        if not worker_exited:
            self._log.warning(
                "skipping plugin %r stop() because its worker is still running",
                name,
            )
            return
        self._stop_plugin(name, plugin, worker._join_timeout)

    # ------------------------------------------------------------------ #
    # discovery / configuration helpers
    # ------------------------------------------------------------------ #

    def _warn_unmatched_enabled_names(self, entry_points):
        """
        Warn for every plugin name that is enabled in properties but has no
        matching entry point -- otherwise the only signal is the host's
        "started 0 plugin(s)" INFO line.
        """
        registered = {name for name, _ep in entry_points}
        for name in sorted(enabled_plugin_names(self._props)):
            if name not in registered:
                self._log.warning(
                    "pegasus.monitord.plugins.%s.enabled is set, but no plugin "
                    "named %r is registered under the %r entry-point group; "
                    "is the plugin's package installed in monitord's environment?",
                    name,
                    name,
                    MONITORD_PLUGIN_ENTRY_POINT_GROUP,
                )

    def _cleanup_failed_start(self, name, plugin, worker, join_timeout):
        worker_exited = True
        if worker is not None:
            try:
                worker_exited = worker.close()
            except Exception:
                worker_exited = False
                self._log.error(
                    "error closing partially-started worker for plugin %r\n%s",
                    name,
                    traceback.format_exc(),
                )
        if not worker_exited:
            # Same rule as stop_all: never race stop() against a worker that
            # may still be inside handle_event.
            self._log.warning(
                "skipping plugin %r stop() during startup cleanup "
                "because its worker is still running",
                name,
            )
            return
        if plugin is not None:
            self._stop_plugin(name, plugin, join_timeout, "startup cleanup")

    def _start_kwargs(self, plugin):
        """
        Keyword arguments beyond ``props`` that this plugin's ``start()``
        accepts. Old-style overrides -- ``start(self, props=None)`` -- get an
        empty dict, so pre-existing third-party plugins are called exactly as
        before the ``restart`` keyword existed.
        """
        extras = {"restart": self._restart}
        try:
            params = inspect.signature(plugin.start).parameters.values()
        except (TypeError, ValueError):
            return {}
        if any(p.kind is p.VAR_KEYWORD for p in params):
            return extras
        accepted = {p.name for p in params}
        return {key: val for key, val in extras.items() if key in accepted}

    def _stop_plugin(self, name, plugin, timeout, context="shutdown"):
        """
        Run plugin.stop() with the same bound used for worker shutdown.
        An ``async def stop()`` is driven on a throwaway event loop. It gets
        one ``timeout`` interval to finish normally; if still running, the
        host requests cancellation and waits one additional ``timeout`` for
        cooperative cancellation cleanup before abandoning the helper.
        """
        done = []
        async_stop = _is_coroutine_callable(plugin.stop)
        cancel_requested = Event()
        async_ready = Event()
        async_state = {}

        def stop_target():
            try:
                if async_stop:
                    self._run_stop_coroutine(
                        plugin,
                        cancel_requested,
                        async_ready,
                        async_state,
                    )
                else:
                    plugin.stop()
            except asyncio.CancelledError:
                # A self-cancelling stop hook is a plugin failure, but it must
                # not escape the helper thread as an unhandled BaseException.
                self._log.error(
                    "plugin %r stop() cancelled itself during %s",
                    name,
                    context,
                )
            except Exception:
                self._log.error(
                    "plugin %r stop() failed during %s\n%s",
                    name,
                    context,
                    traceback.format_exc(),
                )
            finally:
                done.append(True)

        stop_thread = Thread(
            target=stop_target,
            name=f"monitord-plugin-{name}-stop",
            daemon=True,
        )
        stop_thread.start()
        stop_thread.join(timeout=timeout)
        if not stop_thread.is_alive():
            return bool(done)

        if async_stop:
            # Request cancellation from the controlling thread so it begins
            # before _stop_plugin returns. An inner wait_for using the same
            # timeout races this outer join and always loses that guarantee.
            cancel_requested.set()
            if async_ready.is_set():
                loop = async_state.get("loop")
                task = async_state.get("task")
                if loop is not None and task is not None:
                    try:
                        loop.call_soon_threadsafe(task.cancel)
                    except RuntimeError:
                        # The task completed and closed its loop between the
                        # liveness check and cancellation request.
                        pass
            stop_thread.join(timeout=timeout)
            if not stop_thread.is_alive():
                self._log.warning(
                    "plugin %r async stop() exceeded %.1fs during %s; cancelled",
                    name,
                    timeout,
                    context,
                )
                return bool(done)
            self._log.warning(
                "plugin %r async stop() did not finish within %.1fs during %s "
                "and did not complete cancellation within another %.1fs; "
                "abandoning it",
                name,
                timeout,
                context,
                timeout,
            )
            return False

        if stop_thread.is_alive():
            self._log.warning(
                "plugin %r stop() did not return within %.1fs during %s; abandoning it",
                name,
                timeout,
                context,
            )
            return False
        return bool(done)

    def _run_stop_coroutine(
        self,
        plugin,
        cancel_requested,
        ready,
        state,
    ):
        """
        Drive an ``async def stop()`` on a throwaway event loop. The caller
        owns the timeout and requests cancellation through shared state so
        the runtime bound and cancellation-grace bound cannot race each other.
        """
        loop = asyncio.new_event_loop()
        task = loop.create_task(plugin.stop())
        state["loop"] = loop
        state["task"] = task
        ready.set()
        if cancel_requested.is_set():
            task.cancel()
        try:
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                if not cancel_requested.is_set():
                    raise
        finally:
            loop.close()

    def _iter_enabled_entry_points(self, entry_points):
        for name, ep in entry_points:
            if not self._is_enabled(name):
                self._log.debug(
                    "monitord plugin %r is registered but not enabled "
                    "(set pegasus.monitord.plugins.%s.enabled=true to enable)",
                    name,
                    name,
                )
                continue
            yield name, ep

    def _worker_config(self, name):
        queue_size = self._int_prop(
            f"pegasus.monitord.plugins.{name}.queue_size", DEFAULT_QUEUE_SIZE
        )
        join_timeout = self._float_prop(
            f"pegasus.monitord.plugins.{name}.join_timeout",
            DEFAULT_JOIN_TIMEOUT,
        )
        start_timeout = self._float_prop(
            f"pegasus.monitord.plugins.{name}.start_timeout",
            join_timeout,
        )
        tick_interval = self._float_prop(
            f"pegasus.monitord.plugins.{name}.tick_interval",
            DEFAULT_TICK_INTERVAL,
        )
        event_timeout = self._float_prop(
            f"pegasus.monitord.plugins.{name}.event_timeout",
            DEFAULT_EVENT_TIMEOUT,
        )
        if start_timeout < 0:
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.start_timeout must be >= 0"
            )
        if join_timeout < 0:
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.join_timeout must be >= 0"
            )
        if event_timeout < 0:
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.event_timeout must be >= 0"
            )
        events = None
        raw = self._raw_prop(f"pegasus.monitord.plugins.{name}.events", None)
        if raw is not None:
            events = tuple(p.strip() for p in str(raw).split(",") if p.strip())
            if not events:
                raise ValueError(
                    f"pegasus.monitord.plugins.{name}.events must name at "
                    f"least one event prefix (use '*' for all events)"
                )
        policy = str(
            self._raw_prop(
                f"pegasus.monitord.plugins.{name}.overflow_policy",
                DEFAULT_OVERFLOW_POLICY,
            )
        )
        policy = policy.strip().lower()
        if policy not in (OVERFLOW_DROP_NEWEST, OVERFLOW_DROP_OLDEST):
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.overflow_policy must be "
                f"{OVERFLOW_DROP_NEWEST!r} or {OVERFLOW_DROP_OLDEST!r}"
            )
        return _WorkerConfig(
            queue_size=queue_size,
            start_timeout=start_timeout,
            join_timeout=join_timeout,
            tick_interval=tick_interval,
            events=events,
            overflow_policy=policy,
            event_timeout=event_timeout,
        )

    def _compile_event_filter(self, name, prop_patterns, plugin):
        """
        Resolve the effective filter: the ``.events`` property replaces the
        plugin's declared ``event_filter``; ``None`` means deliver everything.
        Read from the *instance* after ``start()`` so plugins may derive it
        dynamically (e.g. from props) in ``start()``.
        """
        patterns = prop_patterns
        if patterns is None:
            declared = getattr(plugin, "event_filter", None)
            if declared is None:
                return None
            if isinstance(declared, str):
                declared = (declared,)
            patterns = tuple(str(p) for p in declared)
        if "*" in patterns:
            return None
        for p in patterns:
            # hard-coded literal: importing STAMPEDE_NS from event_output
            # would be circular (event_output imports this module)
            if not p.startswith("stampede."):
                self._log.warning(
                    "plugin %r event filter pattern %r does not start with "
                    "'stampede.'; plugins see fully-qualified event names, "
                    "so this pattern will match nothing",
                    name,
                    p,
                )
        return patterns

    def _discover_entry_points(self):
        """
        Return a list of (name, EntryPoint) for the plugin group. Degrades
        gracefully to an empty list on Python < 3.8 (no importlib.metadata) or
        on any discovery error.
        """
        try:
            import importlib.metadata as importlib_metadata
        except ImportError:
            self._log.warning(
                "monitord plugins require Python 3.8+ (importlib.metadata); "
                "plugin discovery disabled"
            )
            return []
        try:
            eps = importlib_metadata.entry_points()
        except Exception:
            self._log.error("error reading entry points\n%s", traceback.format_exc())
            return []
        # Python 3.10+: EntryPoints.select(group=...)
        # Python 3.8/3.9: entry_points() returns a dict keyed by group
        if hasattr(eps, "select"):
            selected = eps.select(group=MONITORD_PLUGIN_ENTRY_POINT_GROUP)
        else:
            selected = eps.get(MONITORD_PLUGIN_ENTRY_POINT_GROUP, [])
        return [(ep.name, ep) for ep in selected]

    def _is_enabled(self, name):
        if self._props is None:
            return False
        val = self._props.property(f"pegasus.monitord.plugins.{name}.enabled")
        return bool(utils.make_boolean(val if val is not None else "false"))

    def _int_prop(self, key, default):
        return int(self._raw_prop(key, default))

    def _float_prop(self, key, default):
        return float(self._raw_prop(key, default))

    def _raw_prop(self, key, default):
        if self._props is None:
            return default
        val = self._props.property(key)
        return default if val is None else val
