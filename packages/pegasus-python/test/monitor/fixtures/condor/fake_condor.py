#!/usr/bin/env python3
"""Fake fixed-name HTCondor executable used by the WP5 contract tests."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path


def append_log() -> None:
    path = os.environ.get("FAKE_CONDOR_LOG")
    if not path:
        return
    record = {
        "executable": Path(sys.argv[0]).name,
        "argv": sys.argv[1:],
        "environment": {
            key: os.environ.get(key)
            for key in (
                "SECRET_PARENT",
                "CONDOR_CONFIG",
                "_CONDOR_SEC_TOKEN_DIRECTORY",
                "X509_USER_CERT",
                "X509_USER_KEY",
                "_CONDOR_PASSWORD_FILE",
            )
        },
    }
    with open(path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(record) + "\n")


append_log()
mode_file = os.environ.get("FAKE_CONDOR_MODE_FILE")
mode = (
    Path(mode_file).read_text(encoding="utf-8").strip()
    if mode_file
    else os.environ.get("FAKE_CONDOR_MODE", "success")
)

if mode == "empty":
    if os.environ.get("FAKE_CONDOR_EMPTY_JSON") == "1":
        sys.stdout.write("[]")
    raise SystemExit(0)

if mode == "malformed":
    sys.stdout.write("{not json")
    raise SystemExit(0)

if mode == "bad_shape":
    sys.stdout.write('{"unexpected": true}')
    raise SystemExit(0)

if mode == "oversized":
    sys.stdout.write('["' + ("x" * 1048576) + '"]')
    sys.stdout.flush()
    raise SystemExit(0)

if mode == "oversized_stderr":
    sys.stderr.write("x" * 1048576)
    sys.stderr.flush()
    time.sleep(60)
    raise SystemExit(0)

if mode == "nonzero":
    sys.stderr.write(os.environ.get("FAKE_CONDOR_ERROR", "command failed"))
    raise SystemExit(int(os.environ.get("FAKE_CONDOR_EXIT", "1")))

if mode == "hang":
    started = os.environ.get("FAKE_CONDOR_STARTED")
    if started:
        Path(started).write_text("started", encoding="utf-8")
    child_marker = os.environ.get("FAKE_CONDOR_CHILD_MARKER")
    child_pid = os.environ.get("FAKE_CONDOR_CHILD_PID")
    child_ready = os.environ.get("FAKE_CONDOR_CHILD_READY")
    child_code = """
import os
import signal
import sys
import time

marker = sys.argv[1]
ready = sys.argv[2]

def stopped(_signum, _frame):
    with open(marker, 'w', encoding='utf-8') as stream:
        stream.write('terminated')
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stopped)
with open(ready, 'w', encoding='utf-8') as stream:
    stream.write('ready')
while True:
    time.sleep(1)
"""
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_code,
            child_marker or os.devnull,
            child_ready or os.devnull,
        ]
    )
    if child_pid:
        Path(child_pid).write_text(str(child.pid), encoding="utf-8")
    time.sleep(60)
    raise SystemExit(0)

payload = os.environ.get("FAKE_CONDOR_PAYLOAD")
payload_file = os.environ.get("FAKE_CONDOR_PAYLOAD_FILE")
if payload_file:
    payload = Path(payload_file).read_text(encoding="utf-8")
if payload is None:
    payload = "[]"
sys.stdout.write(payload)
