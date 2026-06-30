#!/usr/bin/env python3
"""
distributed_memtier.py

Starts a server (fleec or memcached) on a remote machine via SSH, prefills it,
then launches memtier_benchmark on multiple client machines in parallel.
A threading barrier ensures every client starts the timed workload at the
same moment. Throughput is aggregated across all client machines.

Usage:
    python3 distributed_memtier.py [options]

Examples:
    # Use defaults (jemalloc, memcached, 2 clients, 1 server)
    python3 distributed_memtier.py

    # Override allocator, server type, and machines
    python3 distributed_memtier.py \\
        --allocator tcmalloc \\
        --server-type memcached \\
        --server-machine larochette-6 \\
        --clients larochette-4 larochette-5 \\
        --allocator-dir /home/acosta/deqalloc/build/allocators \\
        --server-bin-dir /home/acosta/deqalloc-rebuttal/fleec/Codigo/fleec/src \\
        --server-bin fleec

    # Change workload knobs
    python3 distributed_memtier.py \\
        --nkeys 5000000 --valuesz 64 --percreads 95 \\
        --nthreads 8 --nclientspthread 50 --duration 30 --pipeline 200
"""

import argparse
import os
import re
import select
import shlex
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

# ─── ANSI helpers ────────────────────────────────────────────────────────────

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def log(msg: str)  -> None: print(f"\033[34m[{_ts()}]\033[0m {msg}", file=sys.stderr)
def ok(msg: str)   -> None: print(f"\033[32m[{_ts()}]\033[0m {msg}", file=sys.stderr)
def warn(msg: str) -> None: print(f"\033[33m[{_ts()}]\033[0m {msg}", file=sys.stderr)
def die(msg: str)  -> None:
    print(f"\033[31m[{_ts()}] ERROR:\033[0m {msg}", file=sys.stderr)
    sys.exit(1)

# ─── Config dataclass ─────────────────────────────────────────────────────────

@dataclass
class Config:
    # Workload
    nkeys:            int   = 10_000_000
    valuesz:          int   = 1
    percreads:        int   = 99
    nthreads:         int   = os.cpu_count() or 4
    nclientspthread:  int   = 30
    duration:         int   = 10
    pipelinesz:       int   = 100

    # Infrastructure
    server_machine:   str        = "larochette-6"
    client_machines:  list[str]  = field(default_factory=lambda: ["larochette-4", "larochette-5"])

    # Server / allocator
    server_bin:       str   = "fleec"       # "fleec" or "memcached"
    server_bin_dir:   str   = "/home/acosta/deqalloc-rebuttal/fleec/Codigo/fleec/src"
    server_type:      str   = "memcache_text"  # redis | memcache_text
    allocator:        str   = "jemalloc"
    allocator_dir:    str   = "../../../build/allocators/"

    # SSH
    ssh_opts:         list[str] = field(default_factory=lambda: [
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        "-o", "BatchMode=yes",
    ])
    barrier_timeout:  int   = 60

    # Derived (set in __post_init__)
    port:             int   = 0
    num_clients:      int   = 0
    workload_ratio:   str   = ""

    def __post_init__(self):
        self.port           = 6379 if self.server_type == "redis" else 11211
        self.num_clients    = len(self.client_machines)
        writes              = 100 - self.percreads
        self.workload_ratio = f"{writes}:{self.percreads}"

    @property
    def ld_preload(self) -> str:
        lib = os.path.join(self.allocator_dir, f"lib{self.allocator}.so")
        return lib

    @property
    def server_bin_path(self) -> str:
        return "memcached"
        return os.path.join(self.server_bin_dir, self.server_bin)

    @property
    def server_config_args(self) -> list[str]:
        return [
            "-s", self.server_machine,
            "-p", str(self.port),
            "-P", self.server_type,
            "-t", str(self.nthreads),
        ]

    @property
    def workload_config_args(self) -> list[str]:
        return [
            f"--pipeline={self.pipelinesz}",
            f"--key-maximum={self.nkeys}",
            "-c", str(self.nclientspthread),
            "-d", str(self.valuesz),
            '--key-prefix=',
        ]

    def max_connections(self) -> int:
        return self.nclientspthread * self.nthreads * self.num_clients * 2


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> Config:
    defaults = Config()

    p = argparse.ArgumentParser(
        description="Distributed memtier benchmark orchestrator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── Workload ──────────────────────────────────────────────────────────────
    wl = p.add_argument_group("Workload")
    wl.add_argument("--nkeys",           type=int, default=defaults.nkeys,
                    help="Number of keys to prefill and benchmark against")
    wl.add_argument("--valuesz",         type=int, default=defaults.valuesz,
                    help="Value size in bytes")
    wl.add_argument("--percreads",       type=int, default=defaults.percreads,
                    help="Percentage of operations that are reads (0-100)")
    wl.add_argument("--nthreads",        type=int, default=defaults.nthreads,
                    help="memtier threads per client machine")
    wl.add_argument("--nclientspthread", type=int, default=defaults.nclientspthread,
                    help="memtier connections (clients) per thread")
    wl.add_argument("--duration",        type=int, default=defaults.duration,
                    help="Benchmark duration in seconds")
    wl.add_argument("--pipeline",        type=int, default=defaults.pipelinesz,
                    help="Pipeline depth (requests in flight)")

    # ── Machines ──────────────────────────────────────────────────────────────
    mc = p.add_argument_group("Machines")
    mc.add_argument("--server-machine",  default=defaults.server_machine,
                    help="Hostname/IP of the server machine")
    mc.add_argument("--clients",         nargs="+", default=defaults.client_machines,
                    metavar="HOST", dest="client_machines",
                    help="One or more client machine hostnames/IPs")

    # ── Server / allocator ────────────────────────────────────────────────────
    sv = p.add_argument_group("Server & allocator")
    sv.add_argument("--server-type",     choices=["redis", "memcache_text"],
                    default=defaults.server_type,
                    help="Protocol to use (redis or memcache_text)")
    sv.add_argument("--server-bin",      default=defaults.server_bin,
                    choices=["fleec", "memcached"],
                    help="Server binary to launch (fleec or memcached)")
    sv.add_argument("--server-bin-dir",  default=defaults.server_bin_dir,
                    help="Directory containing the server binary")
    sv.add_argument("--allocator",       default=defaults.allocator,
                    help="Allocator name; lib<name>.so is loaded via LD_PRELOAD")
    sv.add_argument("--allocator-dir",   default=defaults.allocator_dir,
                    help="Directory that contains lib<allocator>.so")

    # ── SSH / misc ────────────────────────────────────────────────────────────
    misc = p.add_argument_group("Misc")
    misc.add_argument("--barrier-timeout", type=int, default=defaults.barrier_timeout,
                      help="Seconds to wait for all clients to reach the barrier")

    args = p.parse_args()

    cfg = Config(
        nkeys            = args.nkeys,
        valuesz          = args.valuesz,
        percreads        = args.percreads,
        nthreads         = args.nthreads,
        nclientspthread  = args.nclientspthread,
        duration         = args.duration,
        pipelinesz       = args.pipeline,
        server_machine   = args.server_machine,
        client_machines  = args.client_machines,
        server_type      = args.server_type,
        server_bin       = args.server_bin,
        server_bin_dir   = args.server_bin_dir,
        allocator        = args.allocator,
        allocator_dir    = args.allocator_dir,
        barrier_timeout  = args.barrier_timeout,
    )
    return cfg


# ─── SSH helpers ──────────────────────────────────────────────────────────────

def ssh_cmd(cfg: Config, host: str, remote_cmd: str) -> list[str]:
    """Build an ssh command list."""
    return ["ssh"] + cfg.ssh_opts + [host, remote_cmd]


def ssh_run(cfg: Config, host: str, remote_cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command on a remote host synchronously and return the result."""
    cmd = ssh_cmd(cfg, host, remote_cmd)
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


# ─── 1. Start server ──────────────────────────────────────────────────────────

def start_server(cfg: Config) -> None:
    log(f"Starting {cfg.server_bin} on {cfg.server_machine}...")

    if cfg.server_type == "redis":
        remote_cmd = (
            "pkill redis-server 2>/dev/null || true; "
            "sleep 0.5; "
            f"redis-server --daemonize yes --port {cfg.port} --save '' --loglevel warning"
        )
    else:
        max_conn = cfg.max_connections()
        ncpu     = "$(nproc)"
        bin_path = cfg.server_bin_path
        ld       = cfg.ld_preload
        remote_cmd = (
            #"pkill -f memcached 2>/dev/null || true; "
            #"pkill -f fleec 2>/dev/null || true; "
            "sleep 0.5; "
            #f"LD_PRELOAD={shlex.quote(ld)} "
            f"{shlex.quote(bin_path)} "
            f"-d -p {cfg.port} -c {max_conn} -t {ncpu} -m {1024 * 1000}"
        )

    ssh_run(cfg, cfg.server_machine, remote_cmd)


# ─── 2. Wait for server port ──────────────────────────────────────────────────

def wait_for_server(cfg: Config) -> None:
    log(f"Waiting for server on {cfg.server_machine}:{cfg.port}...")
    for i in range(30):
        try:
            with socket.create_connection((cfg.server_machine, cfg.port), timeout=2):
                ok("Server is up")
                return
        except OSError:
            time.sleep(1)
    die("Server did not become ready within 30 s")


# ─── 3. Prefill ───────────────────────────────────────────────────────────────

def prefill(cfg: Config) -> None:
    client = cfg.client_machines[0]
    log(f"Prefilling {cfg.nkeys} keys from {client}...")

    args = (
        ["memtier_benchmark"]
        + cfg.server_config_args
        + cfg.workload_config_args
        + ["--ratio=1:0", "-n", "allkeys", "--key-pattern=P:P"]
    )
    remote_cmd = " ".join(shlex.quote(a) for a in args) + " > /dev/null"
    ssh_run(cfg, client, remote_cmd)
    ok("Prefill done")


# ─── 4. Parse memtier output ──────────────────────────────────────────────────
#
# memtier "Totals" line (--out-file or stdout):
#   Type         Ops/sec    Hits/sec   Misses/sec  ...  Throughput
#   Totals     123456.78   121234.00     2222.78  ...   123.45 MB/sec
#
# We also support MGET by passing --multi-key-get=N which memtier will use
# for GET operations when the protocol is memcache_text.

@dataclass
class ClientStats:
    host:         str
    ops_sec:      float = 0.0
    hits_sec:     float = 0.0
    misses_sec:   float = 0.0
    avg_lat_ms:   float = 0.0
    p99_lat_ms:   float = 0.0
    throughput_mb: float = 0.0
    raw_output:   str   = ""

def parse_totals(output: str) -> Optional[dict]:
    """Extract the Totals row from memtier output."""
    results = {}
    
    # Helper to gracefully handle the '---' empty metrics
    def safe_float(val: str) -> Optional[float]:
        return None if val == "---" else float(val)

    for line in output.splitlines():
        m = re.match(
            r"^\s*(Sets|Gets|Totals)\s+"
            r"(\S+)\s+"  # Col 1: Ops/sec
            r"(\S+)\s+"  # Col 2: Hits/sec
            r"(\S+)\s+"  # Col 3: Misses/sec
            r"(\S+)\s+"  # Col 4: Avg. Latency
            r"(\S+)\s+"  # Col 5: p50 Latency
            r"(\S+)\s+"  # Col 6: p99 Latency
            r"(\S+)\s+"  # Col 7: p99.9 Latency
            r"(\S+)\s*", # Col 8: KB/sec
            line
        )
        if m:
            row_name = m.group(1).lower()  # 'sets', 'gets', or 'totals'
            kb_sec = safe_float(m.group(9))
            
            results[row_name] = {
                "ops_sec":       safe_float(m.group(2)),
                "hits_sec":      safe_float(m.group(3)),
                "misses_sec":    safe_float(m.group(4)),
                "avg_latency":   safe_float(m.group(5)),
                "p50_latency":   safe_float(m.group(6)),
                "p99_latency":   safe_float(m.group(7)),
                "p999_latency":  safe_float(m.group(8)),
                "kb_sec":        kb_sec,
                "throughput_mb": (kb_sec / 1024.0) if kb_sec else 0.0
            }

    return results

    ## Fallback: also try "ALL STATS" style sometimes emitted
    #for line in output.splitlines():
    #    m2 = re.match(r"^\|\s*Totals\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|", line)
    #    if m2:
    #        return {"ops_sec": float(m2.group(1)), "hits_sec": float(m2.group(2)),
    #                "misses_sec": 0.0, "p99_lat_ms": 0.0, "throughput_mb": 0.0}
    #return None


# ─── 5. Distributed benchmark with barrier ────────────────────────────────────
#
# Python equivalent of the bash FIFO barrier, implemented with threads:
#
#   Main thread                         Worker thread (one per client)
#   ───────────                         ──────────────────────────────
#   threading.Barrier(n_clients)        ssh … "printf READY; read _; exec memtier"
#   worker threads launched             subprocess.Popen (stdin=PIPE, stdout=PIPE)
#   wait for READY on stderr pipe       writes "READY\n" to stderr
#   barrier.wait()  ← all arrive        ...blocks on stdin read...
#   write "GO\n" to each stdin          reads "GO\n" → exec memtier
#   all memtiers start ≈ simultaneously

def run_benchmark(cfg: Config) -> list[ClientStats]:
    log(f"Launching {cfg.num_clients} clients with barrier sync...")

    # Build the memtier command run on each client.
    # --multi-key-get=N enables MGET batching on the GET side.
    mget_n  = cfg.pipelinesz  # group GETs into MGET of pipeline size
    mt_args = (
        ["memtier_benchmark"]
        + cfg.server_config_args
        + cfg.workload_config_args
        + [
            f"--ratio={cfg.workload_ratio}",
            f"--test-time={cfg.duration}",
            "--key-pattern=Z:Z",
        ]
    )
    # Add MGET support for memcached protocol
    if cfg.server_type == "memcache_text":
        mt_args += [f"--multi-key-get={mget_n}"]

    mt_cmd_str = " ".join(shlex.quote(a) for a in mt_args)

    # Shell script sent to each remote client.
    # It signals readiness on fd 2 (stderr), waits for GO on stdin, then execs.
    client_script = (
        "printf 'BENCH_READY\\n' >&2; "
        "read -r _go; "
        f"exec {mt_cmd_str}"
    )

    ssh_base = ["ssh"] + cfg.ssh_opts

    processes: list[subprocess.Popen] = []
    stdout_data: list[bytes] = [b""] * cfg.num_clients
    stderr_data: list[bytes] = [b""] * cfg.num_clients

    # ── Launch all SSH processes ───────────────────────────────────────────────
    for host in cfg.client_machines:
        cmd = ssh_base + [host, client_script]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        processes.append(proc)
        log(f"  Launched {host} (pid {proc.pid})")

    # ── Wait for BENCH_READY from every client ────────────────────────────────
    log(f"Waiting for all {cfg.num_clients} clients (timeout: {cfg.barrier_timeout}s)...")
    deadline = time.monotonic() + cfg.barrier_timeout

    for i, (host, proc) in enumerate(zip(cfg.client_machines, processes)):
        ready_buf = b""
        while b"BENCH_READY" not in ready_buf:
            if time.monotonic() > deadline:
                die(f"Timeout waiting for {host} to reach barrier")
            if proc.poll() is not None:
                out, err = proc.communicate()
                die(
                    f"{host}: SSH process exited (rc={proc.returncode}) before reaching barrier\n"
                    f"stdout: {out.decode(errors='replace')}\n"
                    f"stderr: {err.decode(errors='replace')}"
                )
            # Non-blocking read from stderr
            assert proc.stderr is not None
            r, _, _ = select.select([proc.stderr], [], [], 0.1)
            if r:
                chunk = os.read(proc.stderr.fileno(), 4096)
                ready_buf += chunk
        ok(f"  {host} ready")
        stderr_data[i] = ready_buf   # save what we've read so far

    # ── Release barrier: send GO to every client ──────────────────────────────
    ok("All clients ready — GO!")
    for proc in processes:
        assert proc.stdin is not None
        proc.stdin.write(b"GO\n")
        proc.stdin.flush()

    # ── Drain stdout/stderr and wait for completion ───────────────────────────
    # Use threads so we don't deadlock on large outputs
    def drain(proc: subprocess.Popen, idx: int) -> None:
        assert proc.stdout is not None
        assert proc.stderr is not None
        out, err = proc.communicate()
        stdout_data[idx] += out
        stderr_data[idx] += err

    drain_threads = [
        threading.Thread(target=drain, args=(proc, i), daemon=True)
        for i, proc in enumerate(processes)
    ]
    for t in drain_threads:
        t.start()
    for t in drain_threads:
        t.join()

    # ── Collect exit codes ────────────────────────────────────────────────────
    all_ok = True
    for i, (host, proc) in enumerate(zip(cfg.client_machines, processes)):
        rc = proc.wait()
        if rc == 0:
            ok(f"  {host} finished")
        else:
            warn(f"  {host} exited with rc={rc}")
            all_ok = False

    # ── Parse per-client stats ────────────────────────────────────────────────
    stats: list[ClientStats] = []
    for i, host in enumerate(cfg.client_machines):
        raw = stdout_data[i].decode(errors="replace")
        cs  = ClientStats(host=host, raw_output=raw)
        parsed = parse_totals(raw)
        if parsed:
            cs.ops_sec       = parsed["sets"]["ops_sec"] + parsed["gets"]["hits_sec"]
            cs.hits_sec      = parsed["totals"]["hits_sec"]
            cs.misses_sec    = parsed["gets"]["misses_sec"]
            cs.p99_lat_ms    = parsed["totals"]["p99_latency"]
            cs.throughput_mb = parsed["totals"]["throughput_mb"]
        else:
            warn(f"Could not parse Totals from {host} — raw output follows:")
            print(raw, file=sys.stderr)
        stats.append(cs)

    if not all_ok:
        sys.exit(1)

    return stats


# ─── 6. Print aggregated results ──────────────────────────────────────────────

def print_results(cfg: Config, stats: list[ClientStats]) -> None:
    sep = "━" * 60
    print(f"\n{sep}")
    print(f"  RESULTS  ({cfg.num_clients} clients × {cfg.nthreads} threads × {cfg.nclientspthread} conns/thread)")
    print(sep)

    # Per-client table
    print(f"\n{'Host':<20} {'Ops/sec':>12} {'Hits/sec':>12} {'Misses/sec':>12} {'p99 lat (ms)':>14} {'Thruput (MB/s)':>15}")
    print("-" * 90)
    for cs in stats:
        if cs.ops_sec:
            print(
                f"{cs.host:<20} {cs.ops_sec:>12,.0f} {cs.hits_sec:>12,.0f} "
                f"{cs.misses_sec:>12,.0f} {cs.p99_lat_ms:>14.2f} {cs.throughput_mb:>15.2f}"
            )
        else:
            print(f"{cs.host:<20}  (no parseable output)")

    # Aggregated row
    total_ops       = sum(cs.ops_sec       for cs in stats)
    total_hits      = sum(cs.hits_sec      for cs in stats)
    total_misses    = sum(cs.misses_sec    for cs in stats)
    avg_p99         = (sum(cs.p99_lat_ms  for cs in stats if cs.p99_lat_ms)
                       / max(1, sum(1 for cs in stats if cs.p99_lat_ms)))
    total_throughput = sum(cs.throughput_mb for cs in stats)

    print("=" * 90)
    print(
        f"{'AGGREGATE':<20} {total_ops:>12,.0f} {total_hits:>12,.0f} "
        f"{total_misses:>12,.0f} {avg_p99:>14.2f} {total_throughput:>15.2f}"
    )
    print(f"\n{sep}\n")


# ─── Cleanup ──────────────────────────────────────────────────────────────────

def cleanup(cfg: Config) -> None:
    log(f"Stopping server on {cfg.server_machine}...")
    try:
        ssh_run(
            cfg, cfg.server_machine,
            "pkill redis-server 2>/dev/null; pkill memcached 2>/dev/null; pkill fleec 2>/dev/null; true",
            check=False,
        )
    except Exception as e:
        warn(f"Server cleanup failed: {e}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = parse_args()

    log(f"Config: server={cfg.server_machine} ({cfg.server_bin}, {cfg.allocator}), "
        f"clients={cfg.client_machines}, "
        f"workload={cfg.workload_ratio}, duration={cfg.duration}s")

    try:
        start_server(cfg)
        wait_for_server(cfg)
        prefill(cfg)
        for i in range(3):
            stats = run_benchmark(cfg)
            print_results(cfg, stats)
    finally:
        cleanup(cfg)


if __name__ == "__main__":
    main()
