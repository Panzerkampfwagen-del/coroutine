#!/usr/bin/env python3
"""Concurrent TCP echo benchmark for the coro echo_server.

Spawns N persistent connections, each repeatedly sending a fixed-size message
and awaiting the echo while measuring round-trip time, then summarizes
connections/s, throughput, and latency percentiles to stdout and results.json.

Examples:
    # Benchmark an already-running server on :8080
    ./bench.py --conns 1000 --duration 5

    # Start ./echo_server itself, benchmark it, then shut it down
    ./bench.py --spawn ../echo_server --conns 1000 --duration 5
"""
import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time


async def _latency_probe(host, port, msg, count):
    """Single sequential connection: isolates true server round-trip latency,
    free of the client-side queuing that dominates the high-concurrency run."""
    reader, writer = await asyncio.open_connection(host, port)
    n = len(msg)
    rtts = []
    for _ in range(count):
        t = time.perf_counter()
        writer.write(msg)
        await writer.drain()
        await reader.readexactly(n)
        rtts.append((time.perf_counter() - t) * 1e6)  # microseconds
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return rtts


async def _run(args):
    msg = b"x" * args.msg_size
    n = len(msg)
    stats = {"load_rtts": [], "mismatches": 0}

    # Phase 1: true latency from one unloaded, sequential connection.
    probe_rtts = await _latency_probe(args.host, args.port, msg, args.probe)

    # Phase 2: open every connection, timing the ramp-up for connections/sec.
    t0 = time.perf_counter()
    conns = await asyncio.gather(
        *(asyncio.open_connection(args.host, args.port) for _ in range(args.conns)))
    connect_secs = time.perf_counter() - t0

    # Phase 3: every connection echoes in a tight loop until the window closes.
    stop = time.perf_counter() + args.duration

    async def loop(reader, writer):
        while time.perf_counter() < stop:
            t = time.perf_counter()
            writer.write(msg)
            await writer.drain()
            try:
                data = await reader.readexactly(n)
            except asyncio.IncompleteReadError:
                break
            stats["load_rtts"].append((time.perf_counter() - t) * 1e6)
            if data != msg:
                stats["mismatches"] += 1

    t_run = time.perf_counter()
    await asyncio.gather(*(loop(r, w) for (r, w) in conns))
    run_secs = time.perf_counter() - t_run

    for (_, w) in conns:
        w.close()
    await asyncio.gather(*(w.wait_closed() for (_, w) in conns),
                         return_exceptions=True)

    return _summarize(args, stats, probe_rtts, connect_secs, run_secs)


def _pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _pctiles(values):
    s = sorted(values)
    if not s:
        return {k: None for k in ("min", "p50", "p90", "p99", "p999", "max")}
    return {
        "min": round(s[0], 2),
        "p50": round(_pct(s, 50), 2),
        "p90": round(_pct(s, 90), 2),
        "p99": round(_pct(s, 99), 2),
        "p999": round(_pct(s, 99.9), 2),
        "max": round(s[-1], 2),
    }


def _summarize(args, stats, probe_rtts, connect_secs, run_secs):
    load_rtts = stats["load_rtts"]
    requests = len(load_rtts)
    payload_bytes = requests * args.msg_size
    return {
        "config": {
            "host": args.host,
            "port": args.port,
            "connections": args.conns,
            "duration_s": args.duration,
            "message_bytes": args.msg_size,
            "probe_requests": args.probe,
        },
        "connections_per_sec": round(args.conns / connect_secs, 1)
        if connect_secs > 0 else None,
        "connect_time_s": round(connect_secs, 4),
        "requests": requests,
        "requests_per_sec": round(requests / run_secs, 1) if run_secs > 0 else None,
        "throughput_mbps": round((payload_bytes / 1e6) / run_secs, 2)
        if run_secs > 0 else None,
        # True round-trip latency, measured by one sequential connection.
        "latency_us": _pctiles(probe_rtts),
        # Latency seen during the N-connection run; dominated by client-side
        # event-loop queuing, so it reflects the load generator, not the server.
        "latency_under_load_us": _pctiles(load_rtts),
        "mismatches": stats["mismatches"],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--conns", type=int, default=1000, help="concurrent connections")
    ap.add_argument("--duration", type=float, default=5.0, help="seconds")
    ap.add_argument("--msg-size", type=int, default=64, help="bytes per echo")
    ap.add_argument("--probe", type=int, default=20000,
                    help="sequential echoes for the unloaded latency probe")
    ap.add_argument("--spawn", metavar="PATH",
                    help="start this echo_server binary, benchmark it, then stop it")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__),
                                                  "results.json"))
    args = ap.parse_args()

    srv = None
    if args.spawn:
        srv = subprocess.Popen([args.spawn, "-p", str(args.port)],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True)
        line = srv.stdout.readline()  # block until the listening banner
        sys.stderr.write("spawned server: " + line)

    try:
        result = asyncio.run(_run(args))
    finally:
        if srv:
            srv.send_signal(signal.SIGINT)
            try:
                srv.wait(timeout=10)
            except subprocess.TimeoutExpired:
                srv.kill()

    result["meta"] = {
        "kernel": os.uname().release,
        "python": sys.version.split()[0],
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")

    print(json.dumps(result, indent=2))
    print(f"\nwrote {args.out}", file=sys.stderr)
    if result["mismatches"]:
        print("WARNING: echo mismatches detected!", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
