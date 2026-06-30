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


class BlockingStartPlugin(MonitordEventPlugin):
    instances = []

    def __init__(self):
        self.start_entered = threading.Event()
        self.release_start = threading.Event()
        self.stop_entered = threading.Event()
        self.started = False
        self.stopped = False
        type(self).instances.append(self)

    def start(self, props=None):
        self.start_entered.set()
        self.release_start.wait(timeout=5)
        self.started = True

    def stop(self):
        self.stop_entered.set()
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


class StopRecordingBlockingPlugin(BlockingPlugin):
    def __init__(self):
        super().__init__()
        self.stopped = False

    def stop(self):
        self.stopped = True


class BlockingStopPlugin(MonitordEventPlugin):
    def __init__(self):
        self.stop_entered = threading.Event()
        self.release_stop = threading.Event()
        self.stopped = False

    def stop(self):
        self.stop_entered.set()
        self.release_stop.wait(timeout=5)
        self.stopped = True


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


class CountingStartPlugin(MonitordEventPlugin):
    started = 0
    stopped = 0

    def start(self, props=None):
        type(self).started += 1

    def stop(self):
        type(self).stopped += 1


class IdentityRecordingPlugin(MonitordEventPlugin):
    """
    Records the *raw* payload object handed to handle_event (not a snapshot) and
    mutates it in place. Two of these let a test prove each plugin receives its
    own isolated copy: the recorded objects must be distinct, and one plugin's
    in-place mutation must not bleed into another plugin's view.
    """

    def __init__(self):
        self.received = []
        self.handled = threading.Event()

    def handle_event(self, event, kw):
        self.received.append(kw)
        kw["touched_by"] = id(self)  # mutate our own payload in place
        self.handled.set()


class NestedMutatingPlugin(MonitordEventPlugin):
    """
    Mutates nested payload values in place. This guards composite-event style
    payloads where values can be lists/dicts, not only scalar fields.
    """

    def __init__(self):
        self.received = []
        self.handled = threading.Event()

    def handle_event(self, event, kw):
        self.received.append(kw)
        kw["invocations"][0]["user"] = id(self)
        kw["multipart"]["attempts"].append(id(self))
        self.handled.set()


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


def test_start_timeout_skips_plugin_and_late_cleans_up(monkeypatch):
    BlockingStartPlugin.instances = []
    _patch_entry_points(
        monkeypatch, {"slowstart": BlockingStartPlugin, "good": RecordingPlugin}
    )
    props = _props(
        {
            "pegasus.monitord.plugins.slowstart.enabled": "true",
            "pegasus.monitord.plugins.slowstart.start_timeout": "0.01",
            "pegasus.monitord.plugins.good.enabled": "true",
        }
    )
    mgr = MonitordPluginManager(props)

    start = time.monotonic()
    assert mgr.discover_and_start() == 1
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"discover_and_start blocked for {elapsed:.2f}s"
    assert len(BlockingStartPlugin.instances) == 1
    slow = BlockingStartPlugin.instances[0]
    assert slow.start_entered.wait(timeout=1)
    assert slow.started is False
    assert isinstance(mgr._workers[0][1], RecordingPlugin)

    slow.release_start.set()
    assert _wait_for(lambda: slow.started is True)
    assert _wait_for(lambda: slow.stop_entered.is_set())
    assert slow.stopped is True

    mgr.stop_all()
    assert mgr._workers == []


def test_entry_point_discovery_error_is_graceful(monkeypatch):
    def _raise():
        raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(importlib.metadata, "entry_points", _raise)
    mgr = MonitordPluginManager(_props({"pegasus.monitord.plugins.x.enabled": "true"}))
    assert mgr.discover_and_start() == 0


def test_invalid_worker_config_skips_plugin_before_start(monkeypatch):
    CountingStartPlugin.started = 0
    CountingStartPlugin.stopped = 0
    _patch_entry_points(monkeypatch, {"badcfg": CountingStartPlugin})
    props = _props(
        {
            "pegasus.monitord.plugins.badcfg.enabled": "true",
            "pegasus.monitord.plugins.badcfg.queue_size": "not-an-int",
        }
    )

    mgr = MonitordPluginManager(props)

    assert mgr.discover_and_start() == 0
    assert mgr._workers == []
    assert CountingStartPlugin.started == 0
    assert CountingStartPlugin.stopped == 0


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


def test_stop_is_skipped_when_worker_misses_join_timeout(monkeypatch):
    _patch_entry_points(monkeypatch, {"slow": StopRecordingBlockingPlugin})
    props = _props(
        {
            "pegasus.monitord.plugins.slow.enabled": "true",
            "pegasus.monitord.plugins.slow.join_timeout": "0.01",
        }
    )
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]
    worker = mgr._workers[0][2]

    mgr.dispatch("stampede.block", {})
    assert plugin.entered.wait(timeout=2)

    mgr.stop_all()

    assert worker._thread.is_alive()
    assert plugin.stopped is False
    plugin.release.set()
    assert _wait_for(lambda: not worker._thread.is_alive())


def test_stop_is_bounded_when_plugin_stop_blocks(monkeypatch):
    _patch_entry_points(monkeypatch, {"slowstop": BlockingStopPlugin})
    props = _props(
        {
            "pegasus.monitord.plugins.slowstop.enabled": "true",
            "pegasus.monitord.plugins.slowstop.join_timeout": "0.01",
        }
    )
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin = mgr._workers[0][1]

    start = time.monotonic()
    mgr.stop_all()
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"stop_all blocked for {elapsed:.2f}s"
    assert plugin.stop_entered.wait(timeout=1)
    assert plugin.stopped is False

    plugin.release_stop.set()
    assert _wait_for(lambda: plugin.stopped is True)


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
    one object. ``_PluginWorker.submit`` must deepcopy the payload at enqueue
    time so each worker gets a stable, isolated view.

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


def test_each_plugin_gets_an_isolated_payload_copy(monkeypatch):
    """
    Cross-plugin isolation (#2194 review, "sharper edge with >=2 plugins").

    dispatch() hands the SAME producer dict to every worker's submit(); the
    per-worker snapshot is the only thing that gives each plugin its own object.
    With two plugins enabled, each must receive a distinct copy -- neither the
    producer's dict nor each other's -- so one plugin mutating its payload inside
    handle_event cannot corrupt another plugin's view (or race two worker threads
    on a single shared dict). Without the snapshot both workers queue the same
    object and this fails deterministically.
    """
    _patch_entry_points(
        monkeypatch, {"a": IdentityRecordingPlugin, "b": IdentityRecordingPlugin}
    )
    props = _props(
        {
            "pegasus.monitord.plugins.a.enabled": "true",
            "pegasus.monitord.plugins.b.enabled": "true",
        }
    )
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    assert len(mgr._workers) == 2
    plugin_a = mgr._workers[0][1]
    plugin_b = mgr._workers[1][1]

    # One producer dict, fanned out to both workers by dispatch().
    producer = {"xwf__id": "abc", "key": "k1", "value": "v1"}
    mgr.dispatch("stampede.rc.meta", producer)

    assert plugin_a.handled.wait(timeout=2)
    assert plugin_b.handled.wait(timeout=2)
    mgr.stop_all()

    seen_a = plugin_a.received[0]
    seen_b = plugin_b.received[0]

    # Each plugin got its own object: not the producer's, and not each other's.
    assert seen_a is not producer
    assert seen_b is not producer
    assert seen_a is not seen_b

    # One plugin's in-place mutation stays confined to its own copy.
    assert seen_a["touched_by"] == id(plugin_a)
    assert seen_b["touched_by"] == id(plugin_b)

    # The producer's own dict is untouched by either worker.
    assert "touched_by" not in producer


def test_nested_payload_values_are_isolated_per_plugin(monkeypatch):
    """
    Composite job events can contain nested mutable values such as invocation
    lists and multipart records. The worker snapshot must isolate those nested
    objects too, not only the top-level payload dict.
    """
    _patch_entry_points(
        monkeypatch, {"a": NestedMutatingPlugin, "b": NestedMutatingPlugin}
    )
    props = _props(
        {
            "pegasus.monitord.plugins.a.enabled": "true",
            "pegasus.monitord.plugins.b.enabled": "true",
        }
    )
    mgr = MonitordPluginManager(props)
    mgr.discover_and_start()
    plugin_a = mgr._workers[0][1]
    plugin_b = mgr._workers[1][1]

    producer = {
        "xwf__id": "abc",
        "invocations": [{"user": "alice"}],
        "multipart": {"attempts": []},
    }
    mgr.dispatch("stampede.job_inst.composite", producer)

    assert plugin_a.handled.wait(timeout=2)
    assert plugin_b.handled.wait(timeout=2)
    mgr.stop_all()

    seen_a = plugin_a.received[0]
    seen_b = plugin_b.received[0]

    assert seen_a is not producer
    assert seen_b is not producer
    assert seen_a is not seen_b
    assert seen_a["invocations"] is not producer["invocations"]
    assert seen_b["invocations"] is not producer["invocations"]
    assert seen_a["multipart"] is not seen_b["multipart"]

    assert seen_a["invocations"][0]["user"] == id(plugin_a)
    assert seen_b["invocations"][0]["user"] == id(plugin_b)
    assert seen_a["multipart"]["attempts"] == [id(plugin_a)]
    assert seen_b["multipart"]["attempts"] == [id(plugin_b)]
    assert producer == {
        "xwf__id": "abc",
        "invocations": [{"user": "alice"}],
        "multipart": {"attempts": []},
    }


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


def test_plugin_endpoint_injection_preserves_existing_user_endpoint():
    eo = pytest.importorskip("Pegasus.monitoring.event_output")
    user_key = f"pegasus.catalog.workflow.{eo.PLUGIN_HOST_ENDPOINT_BASE}.url"
    props = _props(
        {
            "pegasus.monitord.plugins.rec.enabled": "true",
            user_key: "file:///tmp/user-owned.bp",
        }
    )

    injected_key = eo.ensure_monitord_plugin_endpoint(props)

    assert props.property(user_key) == "file:///tmp/user-owned.bp"
    assert injected_key == (
        f"pegasus.catalog.workflow.{eo.PLUGIN_HOST_ENDPOINT_BASE}_1.url"
    )
    assert props.property(injected_key) == eo.PLUGIN_HOST_URL


def test_replay_purge_detection_survives_plugin_multiplex():
    eo = pytest.importorskip("Pegasus.monitoring.event_output")
    db_sink = object.__new__(eo.DBEventSink)
    db_sink._namespace = eo.STAMPEDE_NS
    mux = object.__new__(eo.MultiplexEventSink)
    mux._endpoints = {"default": db_sink}

    assert eo.should_purge_workflow_database(True, mux) is True
    assert eo.should_purge_workflow_database(False, mux) is False


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


# --------------------------------------------------------------------------- #
# tick() — wall-clock callbacks on the existing worker thread
# --------------------------------------------------------------------------- #


class TickingPlugin(MonitordEventPlugin):
    def __init__(self):
        self.events = []
        self.ticks = []
        self.stopped = False
        self.raise_in_tick = False

    def handle_event(self, event, kw):
        self.events.append(event)

    def tick(self):
        self.ticks.append(time.monotonic())
        if self.raise_in_tick:
            raise RuntimeError("boom in tick")

    def stop(self):
        self.stopped = True


class SlowTickingPlugin(TickingPlugin):
    """handle_event is slow enough that a preloaded queue keeps the worker
    saturated for several tick intervals — exercising the starvation guard."""

    def handle_event(self, event, kw):
        time.sleep(0.02)
        self.events.append(event)


def _ticking_manager(monkeypatch, plugin_cls, extra_props=None):
    _patch_entry_points(monkeypatch, {"tk": plugin_cls})
    d = {"pegasus.monitord.plugins.tk.enabled": "true"}
    d.update(extra_props or {})
    mgr = MonitordPluginManager(_props(d))
    mgr.discover_and_start()
    return mgr, mgr._workers[0][1], mgr._workers[0][2]


def test_ticks_fire_on_idle_queue(monkeypatch):
    mgr, plugin, worker = _ticking_manager(
        monkeypatch,
        TickingPlugin,
        {"pegasus.monitord.plugins.tk.tick_interval": "0.05"},
    )
    # no events at all: ticks must fire from queue idleness alone
    assert _wait_for(lambda: len(plugin.ticks) >= 2)
    mgr.stop_all()
    assert plugin.stopped is True
    assert not worker._thread.is_alive()


def test_no_ticks_without_property(monkeypatch):
    mgr, plugin, worker = _ticking_manager(monkeypatch, TickingPlugin)
    mgr.dispatch("stampede.e0", {})
    mgr.dispatch("stampede.e1", {})
    assert _wait_for(lambda: len(plugin.events) == 2)
    time.sleep(0.15)
    # default tick_interval is 0 -> the blocking-get loop, no ticks ever
    assert plugin.ticks == []
    mgr.stop_all()


def test_tick_exception_does_not_kill_worker(monkeypatch):
    mgr, plugin, worker = _ticking_manager(
        monkeypatch,
        TickingPlugin,
        {"pegasus.monitord.plugins.tk.tick_interval": "0.05"},
    )
    plugin.raise_in_tick = True
    assert _wait_for(lambda: len(plugin.ticks) >= 1)
    # a raising tick must not kill the worker or block event delivery
    mgr.dispatch("stampede.after-tick", {})
    assert _wait_for(lambda: plugin.events == ["stampede.after-tick"])
    assert worker._thread.is_alive()
    mgr.stop_all()
    assert plugin.stopped is True


def test_ticks_interleave_under_continuous_flow(monkeypatch):
    mgr, plugin, worker = _ticking_manager(
        monkeypatch,
        SlowTickingPlugin,
        {"pegasus.monitord.plugins.tk.tick_interval": "0.05"},
    )
    # ~30 events x 0.02s handler = ~0.6s of continuous flow: get() never times
    # out, so only the starvation guard can fire ticks in this window.
    n = 30
    for i in range(n):
        mgr.dispatch(f"stampede.e{i}", {})
    assert _wait_for(lambda: len(plugin.events) == n, timeout=5.0)
    assert len(plugin.ticks) >= 1, "starvation guard never ticked"
    # delivery order is untouched by interleaved ticks
    assert plugin.events == [f"stampede.e{i}" for i in range(n)]
    mgr.stop_all()


def test_stop_all_drains_and_joins_ticking_plugin(monkeypatch):
    mgr, plugin, worker = _ticking_manager(
        monkeypatch,
        TickingPlugin,
        {"pegasus.monitord.plugins.tk.tick_interval": "0.05"},
    )
    n = 5
    for i in range(n):
        mgr.dispatch(f"stampede.e{i}", {})
    mgr.stop_all()
    # FIFO drain: every event ahead of the sentinel was processed
    assert plugin.events == [f"stampede.e{i}" for i in range(n)]
    assert plugin.stopped is True
    assert not worker._thread.is_alive()
    # no tick can fire after the sentinel has been drained
    ticks_at_stop = len(plugin.ticks)
    time.sleep(0.2)
    assert len(plugin.ticks) == ticks_at_stop
