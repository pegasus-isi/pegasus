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

# Queue-overflow policies (pegasus.monitord.plugins.<name>.overflow_policy).
OVERFLOW_DROP_NEWEST = "drop-newest"
OVERFLOW_DROP_OLDEST = "drop-oldest"
DEFAULT_OVERFLOW_POLICY = OVERFLOW_DROP_NEWEST

# Distinct from None, which could in principle be a queued value.
_NOTHING = object()

_WorkerConfig = collections.namedtuple(
    "_WorkerConfig",
    "queue_size start_timeout join_timeout tick_interval events overflow_policy",
)


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
      event worker exits, also in a bounded helper thread.
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
        """

    def stop(self):
        """
        Called once after all events have been processed and this plugin's
        background thread has been joined. If the worker does not exit within
        ``join_timeout``, Pegasus skips ``stop()`` rather than racing cleanup
        against a still-running ``handle_event()``. If ``stop()`` itself does
        not return within ``join_timeout``, Pegasus logs and continues exit.
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
    ):
        self._name = name
        self._plugin = plugin
        self._log = logging.getLogger(f"{__name__}._PluginWorker.{name}")
        self._join_timeout = join_timeout
        self._tick_interval = tick_interval
        self._event_filter = event_filter
        self._overflow_policy = overflow_policy
        # queue_size <= 0 means unbounded (matches queue.Queue default)
        maxsize = queue_size if queue_size and queue_size > 0 else 0
        self._queue = queue.Queue(maxsize=maxsize)
        self._dropped = 0
        self._filtered = 0
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

        The payload is snapshotted with ``copy.deepcopy(kw)`` before it is
        queued. The worker thread reads the payload asynchronously, while
        monitord's main thread keeps -- and in places reuses/mutates -- the
        original dict (e.g. the per-LFN ``rc.meta`` loop in ``workflow.py``
        overwrites ``key``/``value`` and re-sends the same dict; ``wf.plan``
        adds ``db_url`` after the event is dispatched). A per-worker copy gives
        each plugin its own isolated, stable payload and removes that
        cross-thread data race, including nested mutable values in composite
        events.

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
            payload = copy.deepcopy(kw)
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
            self._plugin.handle_event(event, kw)
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
            self._plugin.tick()
        except Exception:
            # Same isolation contract as handle_event: a failing tick is
            # logged, never fatal to the worker.
            self._log.error(
                "plugin %r tick() raised:\n%s",
                self._name,
                traceback.format_exc(),
            )

    def _run(self):
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
        # _dropped/_filtered are written only by submit() and read only here;
        # both run on monitord's main thread (dispatch via the sink's send,
        # close via the atexit sink close), so no lock is needed and the
        # totals are final.
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
        """
        entry_points = self._discover_entry_points()
        self._warn_unmatched_enabled_names(entry_points)
        for name, cls in self._iter_enabled_plugin_classes(entry_points):
            plugin = None
            worker = None
            join_timeout = DEFAULT_JOIN_TIMEOUT
            try:
                cfg = self._worker_config(name)
                join_timeout = cfg.join_timeout
                plugin = cls()
                if not self._start_plugin(name, plugin, cfg.start_timeout):
                    plugin = None
                    continue
                # read from the instance after start() so plugins may derive
                # their filter dynamically (e.g. from props) in start()
                event_filter = self._compile_event_filter(name, cfg.events, plugin)
                worker = _PluginWorker(
                    name,
                    plugin,
                    queue_size=cfg.queue_size,
                    join_timeout=cfg.join_timeout,
                    tick_interval=cfg.tick_interval,
                    event_filter=event_filter,
                    overflow_policy=cfg.overflow_policy,
                )
                worker.start()
                self._workers.append((name, plugin, worker))
                self._log.info("started monitord event plugin %r", name)
            except Exception:
                self._log.error(
                    "failed to start plugin %r; skipping\n%s",
                    name,
                    traceback.format_exc(),
                )
                self._cleanup_failed_start(name, plugin, worker, join_timeout)
        return len(self._workers)

    def dispatch(self, event, kw):
        """
        Hand an event to every running plugin's worker. Non-blocking.
        """
        for _name, _plugin, worker in self._workers:
            worker.submit(event, kw)

    def stop_all(self):
        """
        Drain and join every plugin worker, then call each plugin's ``stop()``
        in a bounded helper thread. Ordering matches the contract: ``stop()``
        starts only after the plugin's event thread has been joined.
        """
        for name, plugin, worker in self._workers:
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
                continue
            self._stop_plugin(name, plugin, worker._join_timeout)
        self._workers = []

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
        if worker is not None:
            try:
                worker.close()
            except Exception:
                self._log.error(
                    "error closing partially-started worker for plugin %r\n%s",
                    name,
                    traceback.format_exc(),
                )
        if plugin is not None:
            self._stop_plugin(name, plugin, join_timeout, "startup cleanup")

    def _start_plugin(self, name, plugin, timeout):
        """
        Run plugin.start() with a bounded wait. Timed-out plugins are skipped.
        """
        done = Event()
        timed_out = Event()
        cleanup_lock = Lock()
        cleanup_started = []
        result = {"ok": False, "traceback": None}
        start_kwargs = self._start_kwargs(plugin)

        def cleanup_after_timeout():
            with cleanup_lock:
                if cleanup_started:
                    return
                cleanup_started.append(True)
            if result["traceback"]:
                self._log.error(
                    "plugin %r start() failed after startup timeout\n%s",
                    name,
                    result["traceback"],
                )
                context = "late startup failure cleanup"
            else:
                self._log.warning(
                    "plugin %r start() returned after startup timeout; running stop() cleanup",
                    name,
                )
                context = "late startup cleanup"
            self._stop_plugin(name, plugin, timeout, context)

        def start_target():
            try:
                plugin.start(self._props, **start_kwargs)
            except Exception:
                result["traceback"] = traceback.format_exc()
            else:
                result["ok"] = True
            finally:
                done.set()
                if timed_out.is_set():
                    cleanup_after_timeout()

        start_thread = Thread(
            target=start_target,
            name=f"monitord-plugin-{name}-start",
            daemon=True,
        )
        start_thread.start()
        start_thread.join(timeout=timeout)
        if start_thread.is_alive():
            timed_out.set()
            self._log.warning(
                "plugin %r start() did not return within %.1fs; skipping it",
                name,
                timeout,
            )
            if done.is_set():
                cleanup_after_timeout()
            return False
        if result["traceback"]:
            self._log.error(
                "failed to start plugin %r; skipping\n%s",
                name,
                result["traceback"],
            )
            self._stop_plugin(name, plugin, timeout, "startup failure cleanup")
            return False
        return True

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
        """
        done = []

        def stop_target():
            try:
                plugin.stop()
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
        if stop_thread.is_alive():
            self._log.warning(
                "plugin %r stop() did not return within %.1fs during %s; abandoning it",
                name,
                timeout,
                context,
            )
            return False
        return bool(done)

    def _iter_enabled_plugin_classes(self, entry_points):
        for name, ep in entry_points:
            if not self._is_enabled(name):
                self._log.debug(
                    "monitord plugin %r is registered but not enabled "
                    "(set pegasus.monitord.plugins.%s.enabled=true to enable)",
                    name,
                    name,
                )
                continue
            try:
                cls = ep.load()
            except Exception:
                self._log.error(
                    "failed to load plugin %r; skipping\n%s",
                    name,
                    traceback.format_exc(),
                )
                continue
            yield name, cls

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
        if start_timeout < 0:
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.start_timeout must be >= 0"
            )
        if join_timeout < 0:
            raise ValueError(
                f"pegasus.monitord.plugins.{name}.join_timeout must be >= 0"
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
