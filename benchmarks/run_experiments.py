#!/usr/bin/env python3
"""
Usage:
    ./run_experiments.py <experiment> [options]

Experiments:
    flock      - Run flock allocator benchmarks (vary allocator, update%, ds, size)
    setbench   - Run setbench allocator+tracker benchmarks (vary allocator, tracker, update%, ds, size)
    all        - Run both flock and setbench experiments

Options:
    --output DIR        Output directory for results
    --flock-dir DIR     Path to flock benchmark binaries (default: ../build/benchmarks/flock)
    --setbench-dir DIR  Path to setbench benchmark binaries (default: ../build/benchmarks/setbench)
    --runs N            Number of runs to average (default: 5)
    --allocator NAME    Run only a specific allocator
    --ds NAME           Run only a specific data structure

Examples:
    ./run_experiments.py flock
    ./run_experiments.py setbench
    ./run_experiments.py all --output ./results
    ./run_experiments.py flock --runs 3 --allocator deqalloc
"""

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class FlockConfig:
    """Configuration for flock allocator benchmarks."""
    # allocator name → (use_numa, ignored_df)
    # Format: "name", "name:numa", "name::df", "name:numa:df"
    allocators_raw: List[str] = field(default_factory=lambda: [
        "deqalloc",
        "mimalloc",
        "mimalloc-batchit",
        "jemalloc:numa:df",
        "snmalloc",
        "hoard:numa",
        "tcmalloc::df",
        "tbbmalloc::df",
        "lockfree:numa:df",
        "rpmalloc::df",
    ])

    update_percs: List[int] = field(default_factory=lambda: [1, 5, 50, 90, 100])

    # (rideable, size) pairs — same grouping as bash script
    rideables_sizes: List[Tuple[str, int]] = field(default_factory=lambda: [
        # small
        ("btree_lck",      5_000),
        ("hash_block_lck", 5_000),
        ("leaftree_lck",   5_000),
        ("skiplist_lck",   5_000),
        ("arttree_lck",    5_000),
        ("list_lck",         500),
        # medium
        ("btree_lck",      200_000),
        ("hash_block_lck", 200_000),
        ("leaftree_lck",   200_000),
        ("skiplist_lck",   200_000),
        ("arttree_lck",    200_000),
        ("list_lck",         2_000),
        # medium-large
        ("btree_lck",      20_000_000),
        ("hash_block_lck", 20_000_000),
        ("leaftree_lck",    2_000_000),
        ("skiplist_lck",    2_000_000),
        ("arttree_lck",     2_000_000),
        ("list_lck",            10_000),
        # large
        ("btree_lck",      200_000_000),
        ("hash_block_lck", 200_000_000),
        ("leaftree_lck",    20_000_000),
        ("skiplist_lck",    20_000_000),
        ("arttree_lck",     20_000_000),
        ("list_lck",             10_000),
    ])

    runs: int = 5
    trial_time_sec: int = 5
    nproc: int = field(default_factory=os.cpu_count)


@dataclass
class SetbenchConfig:
    """Configuration for setbench ubench allocator+tracker benchmarks."""
    allocators_raw: List[str] = field(default_factory=lambda: [
        "deqalloc",
        "mimalloc",
        "mimalloc-batchit",
        "jemalloc:numa:df",
        "snmalloc",
        "hoard:numa",
        "tcmalloc::df",
        "tbbmalloc::df",
        "lockfree:numa:df",
        "rpmalloc::df",
    ])

    trackers: List[str] = field(default_factory=lambda: [
        "2geibr", "debra", "he", "ibr_hp", "ibr_rcu",
        "nbr", "nbrplus", "qsbr", "wfe",
    ])

    update_percs: List[int] = field(default_factory=lambda: [1, 5, 50, 90, 100])

    rideables_sizes: List[Tuple[str, int]] = field(default_factory=lambda: [
        # small
        ("guerraoui_ext_bst_ticket",   5_000),
        ("brown_ext_abtree_lf",        5_000),
        ("hm_hashtable",               5_000),
        ("hmlist",                       500),
        # medium
        ("guerraoui_ext_bst_ticket",   200_000),
        ("brown_ext_abtree_lf",        200_000),
        ("hm_hashtable",               200_000),
        ("hmlist",                       2_000),
        # medium-large
        ("guerraoui_ext_bst_ticket",   20_000_000),
        ("brown_ext_abtree_lf",        20_000_000),
        ("hm_hashtable",                2_000_000),
        ("hmlist",                         10_000),
        # large
        ("guerraoui_ext_bst_ticket",   200_000_000),
        ("brown_ext_abtree_lf",        200_000_000),
        ("hm_hashtable",                20_000_000),
        ("hmlist",                         10_000),
    ])

    runs: int = 5
    trial_time_ms: int = 5000
    nproc: int = field(default_factory=os.cpu_count)


# ---------------------------------------------------------------------------
# Allocator helpers
# ---------------------------------------------------------------------------

def parse_allocator(raw: str) -> Tuple[str, bool, bool]:
    """Parse 'name[:numa][:df]' → (name, use_numa, use_df)."""
    parts = raw.split(":")
    name     = parts[0]
    use_numa = len(parts) > 1 and parts[1] == "numa"
    use_df   = len(parts) > 2 and parts[2] == "df"
    return name, use_numa, use_df


def find_allocator_lib(alloc_dir: str, name: str) -> Optional[str]:
    lib = os.path.join(alloc_dir, f"lib{name}.so")
    return lib if os.path.isfile(lib) else None


# ---------------------------------------------------------------------------
# Benchmark runner helpers
# ---------------------------------------------------------------------------

def run_command(cmd: str, timeout_sec: int = 600) -> Tuple[str, str]:
    """Run a shell command, streaming output. Returns (output, status)."""
    try:
        proc = subprocess.Popen(
            cmd, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        lines = []
        for line in proc.stdout:
            print(line, end="")
            lines.append(line)
        try:
            proc.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            proc.kill()

        output = "".join(lines)
        rc = proc.returncode
        if rc in (124, 137):
            print(f"  TIMEOUT ({timeout_sec}s): {cmd}", file=sys.stderr)
            return output, "timeout"
        elif rc != 0:
            print(f"  CRASH (exit={rc}): {cmd}", file=sys.stderr)
            return output, "crash"
        return output, "ok"
    except Exception as e:
        print(f"  ERROR: {cmd}: {e}", file=sys.stderr)
        return "", "error"


def get_machine_info() -> str:
    try:
        result = subprocess.run(["lscpu"], capture_output=True, text=True)
        for line in result.stdout.split("\n"):
            if "Model name" in line:
                return line.strip()[-5:].replace(" ", "")
    except Exception:
        pass
    return "unknown"


# ---------------------------------------------------------------------------
# Output / results file helpers
# ---------------------------------------------------------------------------

class ResultsFile:
    def __init__(self, path: str, experiment: str):
        today = datetime.now().strftime("%m-%d-%Y")
        self.f = open(path, "w")
        self.f.write(f"Date: {today}\n")
        self.f.write(f"Experiment: {experiment}\n")
        try:
            result = subprocess.run(["lscpu"], capture_output=True, text=True)
            for line in result.stdout.split("\n"):
                if "Model name" in line:
                    self.f.write(f"{line.strip()}\n")
                    break
        except Exception:
            pass
        try:
            git = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True)
            self.f.write(f"Git: {git.stdout.strip()}\n")
        except Exception:
            pass
        self.f.write("---\n")
        self.f.flush()

    def write(self, line: str):
        self.f.write(line)
        self.f.flush()

    def close(self):
        self.f.close()


# ---------------------------------------------------------------------------
# Flock experiment runner
# ---------------------------------------------------------------------------

class FlockRunner:

    def __init__(self, config: FlockConfig, bench_dir: str, alloc_dir: str,
                 output_dir: Optional[str]):
        self.config    = config
        self.bench_dir = bench_dir
        self.alloc_dir = alloc_dir
        self.output_dir = output_dir

    def _output_path(self, name: str) -> str:
        today   = datetime.now().strftime("%m-%d-%Y")
        machine = get_machine_info()
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
            return os.path.join(self.output_dir, name)
        return f"../../timings/{name}_{self.config.nproc}-{machine}_{today}"

    def _binary(self, rideable: str) -> Optional[str]:
        path = os.path.join(self.bench_dir, rideable)
        return path if os.path.isfile(path) else None

    def run(self):
        cfg = self.config
        path = self._output_path("flock_allocators")
        print(f"Output: {path}")
        rf = ResultsFile(path, "flock_allocators")

        fmt = "{:<12} {:<10} {:<20} {:<10} {:<6} {}"
        header = fmt.format("allocator", "update%", "ds", "key_size", "numa", "results")
        rf.write(header + "\n")
        print(header)

        for rideable, size in cfg.rideables_sizes:
            binary = self._binary(rideable)
            if binary is None:
                continue

            for update_perc in cfg.update_percs:
                for raw_alloc in cfg.allocators_raw:
                    alloc, use_numa, _ = parse_allocator(raw_alloc)
                    lib = find_allocator_lib(self.alloc_dir, alloc)
                    if lib is None:
                        print(f"  WARNING: allocator lib not found: {alloc}", file=sys.stderr)
                        continue

                    numa_cmd = "numactl -i all" if use_numa else ""
                    prefix = fmt.format(alloc, update_perc, rideable, size, str(use_numa), "[ ")
                    print(prefix, end="", flush=True)
                    rf.write(prefix)

                    # flock binary accepts -r for multiple runs internally
                    half_n = size // 2
                    cmd = (
                        f"PARLAY_NUM_THREADS={cfg.nproc} "
                        f"LD_PRELOAD={lib} "
                        f'/usr/bin/time -f "%M KiloBytes" '
                        f"{numa_cmd} "
                        f"{binary} "
                        f"-p {cfg.nproc} -u {update_perc} -n {half_n} "
                        f"-t {cfg.trial_time_sec} -r {cfg.runs}"
                    ).strip()

                    output, status = run_command(cmd)

                    # parse — binary emits one mops line per run then a summary
                    output = re.sub(r"linebreak", "\n", output)
                    memusage_kb = None
                    m = re.search(r"(\d+)\s+KiloBytes", output)
                    if m:
                        memusage_kb = int(m.group(1))

                    tps = []
                    for line in output.split("\n"):
                        # last comma-separated field after the last comma is mops
                        m2 = re.search(r",\s*([\d.]+)\s*$", line)
                        if m2:
                            try:
                                tps.append(float(m2.group(1)))
                            except ValueError:
                                pass

                    tps_str = " ".join(f"{v}" for v in tps)
                    tp_avg = sum(tps) / len(tps) if tps else 0.0
                    mem_str = f"{memusage_kb} KB" if memusage_kb is not None else "N/A"
                    suffix = f"{tps_str}] {tp_avg:.4f}, {mem_str}\n"
                    print(suffix, end="")
                    rf.write(suffix)

                    if status != "ok":
                        rf.write(f"# {status.upper()}: {rideable} alloc={alloc} u={update_perc} n={size}\n")

        rf.close()
        print(f"Flock results written to: {path}")


# ---------------------------------------------------------------------------
# Setbench experiment runner
# ---------------------------------------------------------------------------

class SetbenchRunner:

    def __init__(self, config: SetbenchConfig, bench_dir: str, alloc_dir: str,
                 output_dir: Optional[str]):
        self.config    = config
        self.bench_dir = bench_dir
        self.alloc_dir = alloc_dir
        self.output_dir = output_dir

    def _output_path(self, name: str) -> str:
        today   = datetime.now().strftime("%m-%d-%Y")
        machine = get_machine_info()
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
            return os.path.join(self.output_dir, name)
        return f"../../timings/{name}_{self.config.nproc}-{machine}_{today}"

    def _binary(self, rideable: str, tracker: str, df: bool) -> Optional[str]:
        df_suffix = "_df" if df else ""
        name = f"ubench_{rideable}.alloc_new.reclaim_{tracker}{df_suffix}.pool_none.out"
        path = os.path.join(self.bench_dir, name)
        return path if os.path.isfile(path) else None

    def run(self):
        cfg = self.config
        path = self._output_path("setbench_allocators")
        print(f"Output: {path}")
        rf = ResultsFile(path, "setbench_allocators")

        fmt = "{:<17} {:<10} {:<10} {:<25} {:<10} {:<6} {}"
        header = fmt.format("allocator", "update%", "scheme", "ds", "key_size", "numa", "results")
        rf.write(header + "\n")
        print(header)

        for rideable, size in cfg.rideables_sizes:
            for tracker in cfg.trackers:
                for update_perc in cfg.update_percs:
                    for raw_alloc in cfg.allocators_raw:
                        alloc, use_numa, use_df = parse_allocator(raw_alloc)
                        df_suffix = "_df" if use_df else ""
                        scheme_label = f"{tracker}{df_suffix}"

                        binary = self._binary(rideable, tracker, use_df)
                        if binary is None:
                            continue

                        lib = find_allocator_lib(self.alloc_dir, alloc)
                        if lib is None:
                            print(f"  WARNING: allocator lib not found: {alloc}", file=sys.stderr)
                            continue

                        numa_cmd = "numactl -i all" if use_numa else ""
                        update_half = round(update_perc / 2, 2)

                        prefix = fmt.format(alloc, update_perc, scheme_label,
                                            rideable, size, str(use_numa), "[ ")
                        print(prefix, end="", flush=True)
                        rf.write(prefix)

                        tp_avg      = 0.0
                        memusage_avg = 0
                        tps         = []
                        last_status = "ok"

                        for _ in range(cfg.runs):
                            cmd = (
                                f"NO_DESTRUCT=1 "
                                f"LD_PRELOAD={lib} "
                                f'/usr/bin/time -f "%M KiloBytes /usr/bin/time output" '
                                f"{numa_cmd} "
                                f"{binary} "
                                f"-nwork {cfg.nproc} -nprefill {cfg.nproc} "
                                f"-i {update_half} -d {update_half} "
                                f"-rq 0 -rqsize 1 -k {size} -nrq 0 "
                                f"-t {cfg.trial_time_ms}"
                            ).strip()

                            output, status = run_command(cmd)
                            if status != "ok":
                                last_status = status

                            # throughput
                            m_tp = re.search(r"total throughput\s*[:\s]\s*([\d.]+)", output)
                            if m_tp:
                                tp = float(m_tp.group(1))
                                tps.append(tp)
                                tp_avg += tp / cfg.runs

                            # memory
                            m_mem = re.search(r"(\d+)\s+KiloBytes /usr/bin/time output", output)
                            if m_mem:
                                memusage_avg += int(m_mem.group(1)) // cfg.runs

                            print(f"{m_tp.group(1) if m_tp else '?'} ", end="", flush=True)

                        tps_str = " ".join(str(v) for v in tps)
                        suffix  = f"{tps_str}] {tp_avg:.4f}, {memusage_avg} KB\n"
                        print(suffix, end="")
                        rf.write(suffix)

                        if last_status != "ok":
                            rf.write(
                                f"# {last_status.upper()}: {rideable} "
                                f"tracker={scheme_label} alloc={alloc} u={update_perc} k={size}\n"
                            )

        rf.close()
        print(f"Setbench results written to: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))

    parser = argparse.ArgumentParser(
        description="Allocator Experiment Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "experiment",
        choices=["flock", "setbench", "all"],
        help="Experiment to run",
    )
    parser.add_argument("--output",       "-o", metavar="DIR",  help="Output directory for results")
    parser.add_argument("--flock-dir",    metavar="DIR",
                        default=os.path.join(script_dir, "../build/benchmarks/flock"),
                        help="Path to flock benchmark binaries")
    parser.add_argument("--setbench-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/benchmarks/setbench/bin"),
                        help="Path to setbench benchmark binaries")
    parser.add_argument("--flock-alloc-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/benchmarks/flock/lib/allocators"),
                        help="Path to allocator .so files for flock")
    parser.add_argument("--setbench-alloc-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/benchmarks/setbench/lib/allocators"),
                        help="Path to allocator .so files for setbench")
    parser.add_argument("--runs",        type=int, default=5,  help="Number of runs (default: 5)")
    parser.add_argument("--allocator",   default=None,         help="Run only this allocator")
    parser.add_argument("--ds",          default=None,         help="Run only this data structure")
    args = parser.parse_args()

    # -- flock config --
    flock_cfg = FlockConfig(runs=args.runs)
    if args.allocator:
        flock_cfg.allocators_raw = [a for a in flock_cfg.allocators_raw
                                    if a.split(":")[0] == args.allocator]
    if args.ds:
        flock_cfg.rideables_sizes = [(r, s) for r, s in flock_cfg.rideables_sizes if r == args.ds]

    # -- setbench config --
    sb_cfg = SetbenchConfig(runs=args.runs)
    if args.allocator:
        sb_cfg.allocators_raw = [a for a in sb_cfg.allocators_raw
                                 if a.split(":")[0] == args.allocator]
    if args.ds:
        sb_cfg.rideables_sizes = [(r, s) for r, s in sb_cfg.rideables_sizes if r == args.ds]

    flock_runner   = FlockRunner(flock_cfg,   args.flock_dir,    args.flock_alloc_dir,    args.output)
    setbench_runner = SetbenchRunner(sb_cfg,  args.setbench_dir, args.setbench_alloc_dir, args.output)

    experiments = {
        "flock":    [(flock_runner.run,    "Flock Allocator Benchmarks")],
        "setbench": [(setbench_runner.run, "Setbench Allocator+Tracker Benchmarks")],
        "all":      [(flock_runner.run,    "Flock Allocator Benchmarks"),
                     (setbench_runner.run, "Setbench Allocator+Tracker Benchmarks")],
    }

    for func, label in experiments[args.experiment]:
        print(f"\n{'='*55}")
        print(f"  {label}")
        print(f"{'='*55}")
        start = time.time()
        func()
        elapsed = time.time() - start
        print(f"Completed in {elapsed:.1f}s ({elapsed/60:.1f}m)")


if __name__ == "__main__":
    main()
