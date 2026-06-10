"""
Tests for the pegasus-monitord plugin system (Pegasus.monitoring.plugin) and
its host sink (Pegasus.monitoring.event_output.PluginHostEventSink).
"""

import importlib.metadata
import logging
import threading
import time

import pytest

from Pegasus.monitoring.plugin import (
    MONITORD_PLUGIN_ENTRY_POINT_GROUP,
    MonitordEventPlugin,
    MonitordPluginManager,
)
from Pegasus.tools import properties


@pytest.fixture(autouse=True)
def _install_trace_level():
    """Pegasus adds a custom TRACE log level at startup (utils.configureLogging);
    the event sinks call logger.trace(). Install it for bare pytest runs."""
    if not hasattr(logging, "TRACE"):
        logging.TRACE = logging.DEBUG - 1
        logging.addLevelName(logging.TRACE, "TRACE")
    cls = logging.getLoggerClass()
    if not hasattr(cls, "trace"):

        def trace(self, message, *args, **kwargs):
            self.log(logging.TRACE, message, *args, **kwargs)

        cls.trace = trace
    yield


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


class _FakeEntryPoint:
    def __init__(self, name, cls):
        self.name = name
        self._cls = cls

    def load(self):
        return self._cls


class _FakeEntryPoints:
    """Mimics importlib.metadata.EntryPoints (3.10+ .select API). plugin.py
    prefers .select when present, so this works on every supported Python."""

    def __init__(self, eps):
        self._eps = eps

    def select(self, group=None):
        if group == MONITORD_PLUGIN_ENTRY_POINT_GROUP:
            return list(self._eps)
        return []


def _patch_entry_points(monkeypatch, mapping):
    """mapping: {entry_point_name: plugin_class}."""
    eps = [_FakeEntryPoint(name, cls) for name, cls in mapping.items()]
    monkeypatch.setattr(
        importlib.metadata, "entry_points", lambda: _FakeEntryPoints(eps)
    )


def _props(d=None):
    return properties.Properties(dict(d or {}))


def _wait_for(predicate, timeout=2.0, interval=0.005):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# --------------------------------------------------------------------------- #
# test plugins
# --------------------------------------------------------------------------- #


class RecordingPlugin(MonitordEventPlugin):
    def __init__(self):
        self.started_with = "UNSET"
        self.events = []
        self.stopped = False

    def start(self, props=None):
        self.started_with = props

    def handle_event(self, event, kw):
        self.events.append((event, dict(kw)))

    def stop(self):
        self.stopped = True


class StartFailsPlugin(MonitordEventPlugin):
    def __init__(self):
        self.stopped = False

    def start(self, props=None):
        raise RuntimeError("boom in start")

    def stop(self):
        self.stopped = True


class FlakyHandlePlugin(MonitordEventPlugin):
    """Raises on a designated event, records the rest."""

    def __init__(self):
        self.events = []
        self.stopped = False

    def handle_event(self, event, kw):
        if event == "stampede.bad":
            raise ValueError("bad event")
        self.events.append(event)

    def stop(self):
        self.stopped = True


class BlockingPlugin(MonitordEventPlugin):
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.handled = []

    def handle_event(self, event, kw):
        self.entered.set()
        self.release.wait(timeout=5)
        self.handled.append(event)


class GatedRecordingPlugin(MonitordEventPlugin):
    """
    Blocks inside the FIRST handle_event until released, then records every
    event as ``(name, snapshot-of-payload)``. Pinning the worker on the first
    event lets a test enqueue a second event and mutate its payload *before*
    the worker thread ever reads it -- the exact ordering that exposes a
    missing payload snapshot.
    """

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.events = []

    def handle_event(self, event, kw):
        if not self.entered.is_set():
            self.entered.set()
            self.release.wait(timeout=5)
        # snapshot at record time; by now a missing copy upstream would
        # already have let the producer's mutation bleed into ``kw``.
        self.events.append((event, dict(kw)))


# --------------------------------------------------------------------------- #
# base class
# --------------------------------------------------------------------------- #


def test_base_class_methods_are_noops():
    p = MonitordEventPlugin()
    # none of these should raise
    p.start(None)
    p.handle_event("stampede.wf.plan", {"xwf__id": "x"})
    p.stop()


# --------------------------------------------------------------------------- #
# discovery + enable gate
# --------------------------------------------------------------------------- #


def test_discovered_but_not_enabled_is_skipped(monkeypatch):
    _patch_entry_points(monkeypatch, {"rec": RecordingPlugin})
    mgr = MonitordPluginManager(_props())  # no enabled prop
    assert mgr.discover_and_start() == 0


def test_enabled_plugin_started_with_full_props(monkeypatch):
    _patch_entry_points(monkeypatch, {"rec": RecordingPlugin})
    props = _props({"pegasus.monitord.plugins.rec.enabled": "true"})
    mgr = MonitordPluginManager(props)
    assert mgr.discover_and_start() == 1
    plugin = mgr._workers[0][1]
    # start() receives the full properties object, not a subset
    assert plugin.started_with is props
    mgr.stop_all()
    assert plugin.stopped is True


def test_start_failure_does_not_abort_other_plugins(monkeypatch):
    _patch_entry_points(monkeypatch, {"bad": StartFailsPlugin, "good": RecordingPlugin})
    props = _props(
        {
            "pegasus.monitord.plugins.bad.enabled": "true",
            "pegasus.monitord.plugins.good.enabled": "true",
        }
    )
    mgr = MonitordPluginManager(props)
    # only the good plugin survives start()
    assert mgr.discover_and_start() == 1
    assert isinstance(mgr._workers[0][1], RecordingPlugin)
    mgr.stop_all()


def test_entry_point_discovery_error_is_graceful(monkeypatch):
    def _raise():
        raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(importlib.metadata, "entry_points", _raise)
    mgr = MonitordPluginManager(_props({"pegasus.monitord.plugins.x.enabled": "true"}))
    assert mgr.discover_and_start() == 0


# --------------------------------------------------------------------------- #
# event delivery
# --------------------------------------------------------------------------- #


def test_events_delivered_verbatim_in_order(monkeypatch):
    _patch_entry_points(monkeypatch, {"rec": RecordingPlugin})
    props = _props({"pegasus.monitord.plugins.rec.enabled": "true"})
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]

    sent = [
        ("stampede.wf.plan", {"xwf__id": "abc", "dax__label": "diamond"}),
        ("stampede.job_inst.main.end", {"xwf__id": "abc", "exitcode": 0}),
    ]
    for event, kw in sent:
        mgr.dispatch(event, kw)

    assert _wait_for(lambda: len(plugin.events) == len(sent))
    mgr.stop_all()

    # names arrive exactly as dispatched; payload (incl. __-separated keys)
    # is passed through unmodified and order is preserved.
    assert plugin.events == sent
    assert plugin.stopped is True


def test_dispatch_with_no_plugins_is_noop():
    mgr = MonitordPluginManager(_props())
    mgr.discover_and_start()
    mgr.dispatch("stampede.wf.plan", {"xwf__id": "x"})  # must not raise
    mgr.stop_all()


# --------------------------------------------------------------------------- #
# error isolation
# --------------------------------------------------------------------------- #


def test_handle_event_exception_is_swallowed(monkeypatch):
    _patch_entry_points(monkeypatch, {"flaky": FlakyHandlePlugin})
    props = _props({"pegasus.monitord.plugins.flaky.enabled": "true"})
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]
    worker = mgr._workers[0][2]

    mgr.dispatch("stampede.bad", {})  # raises inside handle_event
    mgr.dispatch("stampede.good", {})  # must still be processed

    assert _wait_for(lambda: plugin.events == ["stampede.good"])
    # the worker thread survived the exception
    assert worker._thread.is_alive()
    mgr.stop_all()
    assert plugin.stopped is True


# --------------------------------------------------------------------------- #
# back-pressure
# --------------------------------------------------------------------------- #


def test_send_never_blocks_and_drops_when_full(monkeypatch):
    _patch_entry_points(monkeypatch, {"slow": BlockingPlugin})
    props = _props(
        {
            "pegasus.monitord.plugins.slow.enabled": "true",
            "pegasus.monitord.plugins.slow.queue_size": "2",
        }
    )
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]
    worker = mgr._workers[0][2]

    # first event is picked up and blocks the worker inside handle_event
    mgr.dispatch("stampede.e0", {})
    assert plugin.entered.wait(timeout=2)

    # queue holds 2 (e1, e2); the rest overflow and are dropped. Each dispatch
    # must return promptly even though the worker is blocked.
    start = time.monotonic()
    for i in range(1, 6):
        mgr.dispatch(f"stampede.e{i}", {})
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"dispatch blocked for {elapsed:.2f}s"
    assert worker._dropped >= 1

    # unblock and let it drain, then shut down cleanly
    plugin.release.set()
    mgr.stop_all()


# --------------------------------------------------------------------------- #
# payload isolation — the queued payload must be snapshotted at submit time
# --------------------------------------------------------------------------- #


def test_payload_is_snapshotted_before_async_mutation(monkeypatch):
    """
    Regression for the cross-thread payload race (#2194 review).

    monitord's main thread hands the event sink the *live* payload dict and, in
    places, keeps mutating it after the event is dispatched: the rc.meta loop in
    workflow.py reuses one dict and overwrites ``key``/``value`` between sends,
    and wf.plan adds ``db_url`` after dispatch. The plugin worker reads the
    payload asynchronously, so without a snapshot it can observe those later
    mutations -- torn/incorrect data, or a corrupted dict if two plugins share
    one object. ``_PluginWorker.submit`` must copy the payload with ``dict(kw)``
    at enqueue time so each worker gets a stable, isolated view.

    The worker is pinned inside the first handle_event so the mutation below is
    guaranteed to happen *before* the worker dequeues the event under test;
    without the snapshot this test deterministically observes the mutated dict.
    """
    _patch_entry_points(monkeypatch, {"gate": GatedRecordingPlugin})
    props = _props({"pegasus.monitord.plugins.gate.enabled": "true"})
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]

    # Pin the worker on a throwaway event so the next event waits in the queue.
    mgr.dispatch("stampede.blocker", {"n": "0"})
    assert plugin.entered.wait(timeout=2)

    # Dispatch the event under test, then mutate the SAME dict the main thread
    # still holds -- both shapes monitord actually produces:
    #   * overwrite an existing key (rc.meta key/value reuse)
    #   * add a new key after dispatch (wf.plan db_url)
    payload = {"xwf__id": "abc", "key": "k1", "value": "v1"}
    mgr.dispatch("stampede.rc.meta", payload)
    payload["key"] = "MUTATED"
    payload["value"] = "MUTATED"
    payload["db_url"] = "sqlite:///leaked.db"

    # Let the worker drain; it reads the queued event only now, after mutation.
    plugin.release.set()
    assert _wait_for(lambda: len(plugin.events) == 2)
    mgr.stop_all()

    # The worker must see the payload exactly as it was at dispatch time.
    event, seen = plugin.events[1]
    assert event == "stampede.rc.meta"
    assert seen == {"xwf__id": "abc", "key": "k1", "value": "v1"}
    # and the snapshot must be a distinct object from the producer's dict
    assert seen is not payload


# --------------------------------------------------------------------------- #
# host sink (event_output) — prepends the namespace and passes raw payload
# --------------------------------------------------------------------------- #


def test_plugin_host_event_sink_qualifies_event_names(monkeypatch):
    eo = pytest.importorskip("Pegasus.monitoring.event_output")
    _patch_entry_points(monkeypatch, {"rec": RecordingPlugin})
    props = _props({"pegasus.monitord.plugins.rec.enabled": "true"})

    sink = eo.PluginHostEventSink("plugins://", props=props)
    plugin = sink._manager._workers[0][1]

    # sinks receive the UNQUALIFIED name; the host must prepend "stampede."
    sink.send("job_inst.main.end", {"xwf__id": "abc", "exitcode": 0})

    assert _wait_for(lambda: len(plugin.events) == 1)
    sink.close()

    event, kw = plugin.events[0]
    assert event == "stampede.job_inst.main.end"
    assert kw == {"xwf__id": "abc", "exitcode": 0}
    assert plugin.stopped is True


# --------------------------------------------------------------------------- #
# attach mechanism — injected plugins:// endpoint flows through the real
# create_wf_event_sink() + MultiplexEventSink machinery
# --------------------------------------------------------------------------- #


def test_factory_attaches_plugin_host_via_multiplex(monkeypatch, tmp_path):
    eo = pytest.importorskip("Pegasus.monitoring.event_output")
    _patch_entry_points(monkeypatch, {"rec": RecordingPlugin})

    # mirrors what pegasus-monitord injects: an extra workflow-catalog .url
    # endpoint pointing at the plugin host, alongside the real (file) sink.
    props = _props(
        {
            "pegasus.catalog.workflow.plugins.url": "plugins://",
            "pegasus.monitord.plugins.rec.enabled": "true",
        }
    )
    dest = str(tmp_path / "monitord.bp")
    sink = eo.create_wf_event_sink(
        dest,
        db_type=eo.connection.DBType.WORKFLOW,
        enc="json",
        props=props,
        # monitord threads the full props through so the host can read
        # pegasus.monitord.plugins.* (the multiplex layer strips them).
        monitord_props=props,
    )

    # the extra .url triggers a multiplex wrapping the file sink + plugin host
    assert isinstance(sink, eo.MultiplexEventSink)
    host = sink._endpoints["plugins"]
    assert isinstance(host, eo.PluginHostEventSink)

    # an event fanned through the multiplex reaches the plugin fully-qualified
    sink.send("wf.plan", {"xwf__id": "abc"})
    plugin = host._manager._workers[0][1]
    assert _wait_for(
        lambda: plugin.events == [("stampede.wf.plan", {"xwf__id": "abc"})]
    )
    sink.close()
    assert plugin.stopped is True


def test_factory_no_plugins_is_plain_sink(tmp_path):
    """No plugin props -> no injected endpoint -> no multiplex, no plugin host
    (the backward-compatible no-op case)."""
    eo = pytest.importorskip("Pegasus.monitoring.event_output")
    props = _props()
    dest = str(tmp_path / "monitord.bp")
    sink = eo.create_wf_event_sink(
        dest, db_type=eo.connection.DBType.WORKFLOW, enc="json", props=props
    )
    assert isinstance(sink, eo.FileEventSink)
    assert not isinstance(sink, eo.PluginHostEventSink)
    sink.close()
