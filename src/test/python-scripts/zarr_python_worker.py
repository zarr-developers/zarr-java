"""Long-lived zarr-python worker for the Java interop tests.

Why this exists: the interop suite used to spawn one ``uv run`` per test case,
and each launch paid the cost of starting an interpreter and importing zarr and
numpy. With ~78 v3 cases alone (each dtype crossed with a couple of codecs, plus
the codec matrix, in both directions) that startup cost dominated the suite --
the arrays themselves are 16 KB and take no measurable time. Batching the Python
side into a single process removes it.

A persistent worker was chosen over "generate every fixture up front in one
script" because it keeps per-test semantics exactly as they were: each test still
triggers its own zarr-python call and asserts on its own result, so a failure is
reported against the test that caused it rather than against an aggregate setup
step.

Protocol: one JSON object per line on stdin, one JSON object per line on stdout.

    -> {"op": "write_v3", "args": ["blosc", "blosclz_shuffle_3", "int32", "/tmp/x"]}
    <- {"ok": true}

    -> {"op": "read_v3", "args": [...]}
    <- {"ok": false, "error": "AssertionError: got: ... but expected: ..."}

    -> {"op": "ping"}      <- {"ok": true}          (readiness handshake)
    -> {"op": "shutdown"}  (no response; process exits)

Anything an operation prints, and any warning it emits, is redirected to stderr
so it cannot corrupt the protocol stream on stdout.
"""

import contextlib
import io
import json
import sys
import traceback

from zarr_fixtures import OPERATIONS


def _handle(request):
    op_name = request.get("op")

    if op_name == "ping":
        return {"ok": True}

    operation = OPERATIONS.get(op_name)
    if operation is None:
        return {"ok": False, "error": f"unknown op: {op_name!r}"}

    args = request.get("args", [])
    captured = io.StringIO()
    try:
        # Keep operation chatter off the protocol stream.
        with contextlib.redirect_stdout(captured):
            operation(*args)
    except Exception:  # noqa: BLE001 - every failure is reported to the caller
        return {"ok": False, "error": traceback.format_exc()}
    finally:
        chatter = captured.getvalue()
        if chatter:
            sys.stderr.write(chatter)
            sys.stderr.flush()

    return {"ok": True}


def main():
    # Hold the real stdout for protocol replies; operations get a redirected one.
    protocol_out = sys.stdout

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except ValueError as exc:
            protocol_out.write(json.dumps({"ok": False, "error": f"bad request: {exc}"}) + "\n")
            protocol_out.flush()
            continue

        if request.get("op") == "shutdown":
            return

        response = _handle(request)
        protocol_out.write(json.dumps(response) + "\n")
        protocol_out.flush()


if __name__ == "__main__":
    main()
