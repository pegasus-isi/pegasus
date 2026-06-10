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

import logging
import queue
import traceback
from threading import Thread

from Pegasus.tools import utils

log = logging.getLogger(__name__)

# Entry-point group third-party packages register their plugins under.
MONITORD_PLUGIN_ENTRY_POINT_GROUP = "pegasus.monitord.plugins"

# Per-plugin defaults (overridable via pegasus.monitord.plugins.<name>.*).
DEFAULT_QUEUE_SIZE = 10000
DEFAULT_JOIN_TIMEOUT = 10.0


class MonitordEventPlugin:
    """
    Base class for a pegasus-monitord event plugin.

    Subclasses override the hooks they care about; all three default to
    no-ops. A plugin is discovered via the ``pegasus.monitord.plugins``
    entry-point group and run only when explicitly enabled in properties.

    Threading contract:

    * :meth:`start` and :meth:`stop` are called once each, on monitord's main
      thread, around the lifetime of the event stream.
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

    def start(self, props=None):
        """
        Called once, before any events flow.

        :param props: the full Pegasus properties object
            (:class:`Pegasus.tools.properties.Properties`). Read plugin config
            from ``pegasus.monitord.plugins.<name>.*`` keys, e.g.::

                cfg = props.propertyset(
                    "pegasus.monitord.plugins.myplugin.", remove=True
                )
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

    def stop(self):
        """
        Called once after all events have been processed and this plugin's
        background thread has been joined.
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
    ):
        self._name = name
        self._plugin = plugin
        self._log = logging.getLogger(f"{__name__}._PluginWorker.{name}")
        self._join_timeout = join_timeout
        # queue_size <= 0 means unbounded (matches queue.Queue default)
        maxsize = queue_size if queue_size and queue_size > 0 else 0
        self._queue = queue.Queue(maxsize=maxsize)
        self._dropped = 0
        self._thread = Thread(
            target=self._run, name=f"monitord-plugin-{name}", daemon=True
        )

    def start(self):
        self._thread.start()

    def submit(self, event, kw):
        """
        Enqueue an event for the plugin thread. Never blocks and never raises:
        a full queue drops the event (counted), and a dead worker is a no-op.
        This guarantees monitord's parse loop is never stalled by a plugin.

        The payload is snapshotted with ``dict(kw)`` before it is queued. The
        worker thread reads the payload asynchronously, while monitord's main
        thread keeps -- and in places reuses/mutates -- the original dict (e.g.
        the per-LFN ``rc.meta`` loop in ``workflow.py`` overwrites ``key``/
        ``value`` and re-sends the same dict; ``wf.plan`` adds ``db_url`` after
        the event is dispatched). A per-worker copy gives each plugin its own
        isolated, stable payload and removes that cross-thread data race. A
        shallow copy suffices because every value monitord produces is an
        immutable scalar (str/int/float/bool/None).
        """
        if not self._thread.is_alive():
            return
        try:
            self._queue.put_nowait((event, dict(kw)))
        except queue.Full:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 1000 == 0:
                self._log.warning(
                    "plugin %r queue full; dropped %d event(s) so far",
                    self._name,
                    self._dropped,
                )

    def _run(self):
        while True:
            item = self._queue.get()
            try:
                if item is self._SENTINEL:
                    return
                event, kw = item
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
            finally:
                self._queue.task_done()

    def close(self):
        """
        Drain queued events, then stop and join the worker thread (bounded by
        ``join_timeout``). Returns once the thread has exited or the timeout
        has elapsed.
        """
        if not self._thread.is_alive():
            return
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


class MonitordPluginManager:
    """
    Daemon-level owner of the enabled monitord event plugins.

    Discovers plugins registered under ``pegasus.monitord.plugins``, filters to
    those explicitly enabled in properties, instantiates and ``start()``s each
    once, gives each its own :class:`_PluginWorker` (thread + queue), and tears
    them all down on shutdown.
    """

    def __init__(self, props=None):
        self._props = props
        self._log = logging.getLogger(f"{__name__}.MonitordPluginManager")
        # list of (name, plugin, worker)
        self._workers = []

    def discover_and_start(self):
        """
        Discover, start, and spin up a worker thread for each enabled plugin.
        Returns the number of plugins started. A plugin that fails to load,
        instantiate, or ``start()`` is logged and skipped -- it never aborts
        the others or monitord itself.
        """
        for name, cls in self._iter_enabled_plugin_classes():
            try:
                plugin = cls()
            except Exception:
                self._log.error(
                    "failed to instantiate plugin %r; skipping\n%s",
                    name,
                    traceback.format_exc(),
                )
                continue
            try:
                plugin.start(self._props)
            except Exception:
                self._log.error(
                    "plugin %r start() failed; skipping\n%s",
                    name,
                    traceback.format_exc(),
                )
                continue
            worker = _PluginWorker(
                name,
                plugin,
                queue_size=self._int_prop(
                    f"pegasus.monitord.plugins.{name}.queue_size", DEFAULT_QUEUE_SIZE
                ),
                join_timeout=self._float_prop(
                    f"pegasus.monitord.plugins.{name}.join_timeout",
                    DEFAULT_JOIN_TIMEOUT,
                ),
            )
            worker.start()
            self._workers.append((name, plugin, worker))
            self._log.info("started monitord event plugin %r", name)
        return len(self._workers)

    def dispatch(self, event, kw):
        """
        Hand an event to every running plugin's worker. Non-blocking.
        """
        for _name, _plugin, worker in self._workers:
            worker.submit(event, kw)

    def stop_all(self):
        """
        Drain and join every plugin worker, then call each plugin's ``stop()``.
        Ordering matches the contract: ``stop()`` runs after the plugin's
        thread has been joined.
        """
        for name, plugin, worker in self._workers:
            try:
                worker.close()
            except Exception:
                self._log.error(
                    "error closing worker for plugin %r\n%s",
                    name,
                    traceback.format_exc(),
                )
            try:
                plugin.stop()
            except Exception:
                self._log.error(
                    "plugin %r stop() failed\n%s", name, traceback.format_exc()
                )
        self._workers = []

    # ------------------------------------------------------------------ #
    # discovery / configuration helpers
    # ------------------------------------------------------------------ #

    def _iter_enabled_plugin_classes(self):
        for name, ep in self._discover_entry_points():
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
