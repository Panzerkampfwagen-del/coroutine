#!/usr/bin/env python3
"""End-to-end test for the distributed microdb: replication, sharding through
the proxy, and primary failover.

Self-contained: speaks RESP over raw sockets, so it needs only python3 and the
built `microdb` / `microdb_proxy` binaries (no redis-cli). Spawns real node
processes on loopback, drives them, asserts invariants, and tears everything
down. Exits 0 on success, nonzero on the first failure.

Usage: tests/dist/dist_test.py [bin_dir]   (bin_dir defaults to repo root)
"""
import os
import socket
import subprocess
import sys
import time

BIN_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(__file__), "..", "..")
BIN_DIR = os.path.abspath(BIN_DIR)
MDB = os.path.join(BIN_DIR, "microdb")
PROXY = os.path.join(BIN_DIR, "microdb_proxy")

procs = {}
fails = 0


def check(name, cond, extra=""):
    global fails
    tag = "PASS" if cond else "FAIL"
    msg = f"  [{tag}] {name}"
    if extra and not cond:
        msg += f"  -> {extra}"
    print(msg, flush=True)
    if not cond:
        fails += 1


def spawn(name, binary, args, errfile=None):
    p = subprocess.Popen([binary] + args, stdout=subprocess.PIPE,
                         stderr=(errfile or subprocess.DEVNULL), text=True)
    procs[name] = p
    p.stdout.readline()  # consume the listening banner
    return p


def kill(name):
    p = procs.pop(name, None)
    if p:
        p.terminate()
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()


def wait_port(port, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return True
        except OSError:
            time.sleep(0.02)
    return False


def fnv1a(s):
    h = 1469598103934665603
    for c in s.encode():
        h ^= c
        h = (h * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return h


def shard(k, n=2):
    return fnv1a(k) % n


class Conn:
    """A minimal buffered RESP client."""

    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), 2)
        self.s.settimeout(3)
        self.buf = b""

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass

    def _more(self):
        d = self.s.recv(65536)
        if not d:
            raise ConnectionError("backend closed")
        self.buf += d

    def _line(self):
        while b"\r\n" not in self.buf:
            self._more()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _need(self, n):
        while len(self.buf) < n:
            self._more()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def reply(self):
        t = self._line()
        k = t[:1]
        if k in (b"+", b"-", b":"):
            v = t[1:].decode()
            return int(v) if k == b":" else v
        if k == b"$":
            n = int(t[1:])
            if n < 0:
                return None
            data = self._need(n + 2)[:-2]
            return data.decode("utf-8", "replace")
        if k == b"*":
            n = int(t[1:])
            if n < 0:
                return None
            return [self.reply() for _ in range(n)]
        raise ValueError(f"bad reply {t!r}")

    def cmd(self, *args):
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            a = a if isinstance(a, (bytes, bytearray)) else str(a).encode()
            out.append(f"${len(a)}\r\n".encode() + a + b"\r\n")
        self.s.sendall(b"".join(out))
        return self.reply()


def one(port, *args):
    c = Conn(port)
    try:
        return c.cmd(*args)
    finally:
        c.close()


def poll(fn, want, tries=80, delay=0.05):
    v = None
    for _ in range(tries):
        v = fn()
        if v == want:
            return v
        time.sleep(delay)
    return v


def section(title):
    print(title, flush=True)


def main():
    if not (os.path.exists(MDB) and os.path.exists(PROXY)):
        print(f"missing binaries in {BIN_DIR} (run `make microdb microdb_proxy`)")
        return 1
    errlog = "/tmp/microdb_proxy_dist.log"
    perr = open(errlog, "w")
    try:
        # Topology: 2 shards, each a primary + one replica, behind the proxy.
        spawn("s0p", MDB, ["-p", "7531"])
        spawn("s0r", MDB, ["-p", "7532", "-r", "127.0.0.1:7531"])
        spawn("s1p", MDB, ["-p", "7533"])
        spawn("s1r", MDB, ["-p", "7534", "-r", "127.0.0.1:7533"])
        for p in (7531, 7532, 7533, 7534):
            assert wait_port(p), f"node {p} did not start"
        spawn("px", PROXY, ["-p", "7530",
                            "-s", "127.0.0.1:7531,127.0.0.1:7532",
                            "-s", "127.0.0.1:7533,127.0.0.1:7534"], perr)
        assert wait_port(7530), "proxy did not start"
        time.sleep(0.3)  # let replicas finish PSYNC

        section("== replication ==")
        one(7531, "set", "pre", "snap")          # before replica is hot
        one(7531, "set", "foo", "bar")
        check("live SET replicates to replica",
              poll(lambda: one(7532, "get", "foo"), "bar") == "bar")
        check("snapshot key present on replica", one(7532, "get", "pre") == "snap")
        check("replica refuses client writes",
              "READONLY" in str(one(7532, "set", "x", "1")).upper())
        one(7531, "set", "ctr", "10")
        one(7531, "incr", "ctr")
        one(7531, "incr", "ctr")
        check("INCR replicates deterministically",
              poll(lambda: one(7532, "get", "ctr"), "12") == "12")
        check("primary INFO shows a connected slave",
              "connected_slaves:1" in one(7531, "info", "replication"))
        check("replica INFO link status up",
              "master_link_status:up" in one(7532, "info", "replication"))
        one(7531, "set", "ackkey", "v")
        check("WAIT 1 acknowledges the replica", one(7531, "wait", 1, 1000) == 1)

        section("== sharding through the proxy ==")
        N = 200
        c = Conn(7530)
        for i in range(N):
            c.cmd("set", f"key:{i}", f"val:{i}")
        c.close()
        s0 = one(7531, "dbsize")
        s1 = one(7533, "dbsize")
        check("keys distributed across both shards", s0 > 0 and s1 > 0,
              f"s0={s0} s1={s1}")
        check("proxy DBSIZE sums shards", one(7530, "dbsize") == N + 4,
              str(one(7530, "dbsize")))  # +4 from the replication section keys
        routing_ok = True
        for i in range(0, N, 13):
            k = f"key:{i}"
            w = shard(k)
            if not (one((7531, 7533)[w], "exists", k) == 1 and
                    one((7531, 7533)[1 - w], "exists", k) == 0):
                routing_ok = False
        check("each key lands only on hash(key)%nshards's primary", routing_ok)
        check("round-trip values through the proxy",
              all(one(7530, "get", f"key:{i}") == f"val:{i}"
                  for i in range(0, N, 11)))
        one(7530, "mset", "a", "1", "b", "2", "c", "3", "d", "4")
        check("MSET/MGET fan out and reassemble",
              one(7530, "mget", "a", "b", "c", "d") == ["1", "2", "3", "4"])
        check("EXISTS multi-key sums", one(7530, "exists", "a", "b", "c", "d",
                                          "nope") == 4)
        check("DEL multi-key counts", one(7530, "del", "a", "b", "c", "d") == 4)
        rk = next(f"rr{i}" for i in range(1000) if shard(f"rr{i}") == 0)
        one(7531, "set", rk, "fromprimary")  # write straight to shard-0 primary
        check("proxy reads (routed to a replica) see replicated writes",
              poll(lambda: one(7530, "get", rk), "fromprimary") == "fromprimary")

        section("== failover ==")
        k0 = next(f"hk{i}" for i in range(1000) if shard(f"hk{i}") == 0)
        one(7530, "set", k0, "before")
        time.sleep(0.3)  # let it replicate to 7532
        check("shard-0 replica holds the value before failover",
              one(7532, "get", k0) == "before")
        kill("s0p")  # take down shard-0 primary
        check("reads survive the primary loss (replica serves them)",
              one(7530, "get", k0) == "before")
        recovered = False
        t0 = time.time()
        while time.time() - t0 < 6:
            if one(7530, "set", k0, "after") == "OK" and \
               one(7530, "get", k0) == "after":
                recovered = True
                break
            time.sleep(0.1)
        check("writes recover after the replica is promoted", recovered)
        check("promoted node is now a master",
              "role:master" in one(7532, "info", "replication"))
        k1 = next(f"jk{i}" for i in range(1000) if shard(f"jk{i}") == 1)
        one(7530, "set", k1, "ok")
        check("the healthy shard is unaffected", one(7530, "get", k1) == "ok")
        perr.flush()
        check("proxy logged the promotion",
              "[failover]" in open(errlog).read())
    finally:
        for name in list(procs):
            kill(name)
        perr.close()

    print(f"\n{'ALL DIST TESTS PASS' if fails == 0 else str(fails) + ' FAILED'}",
          flush=True)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
