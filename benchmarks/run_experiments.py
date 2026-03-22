#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple, Dict

# ---------------------------------------------------------------------------
# TODO
#   - IBR benchmarks (?)
# ---------------------------------------------------------------------------

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
            #print(line, end="")
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

#requires numactl, which we depend on anyway
def get_num_numa_nodes() -> int:
    out = subprocess.check_output(["numactl", "--hardware"], text=True)
    m = re.search(r'available:\s+(\d+)\s+nodes', out)
    return int(m.group(1))

#get a reasonable thread sequence taking number of numa sockets into account
#author: claude.ai
def get_numa_aware_thread_sequence(nproc: int, num_nodes: int) -> List[int]:
    threads_per_node = nproc // num_nodes
    points = set()
    p = 1
    while p <= nproc:
        points.add(p)
        p *= 2
    for n in range(1, num_nodes + 1):
        points.add(n * threads_per_node)
    points.add(nproc)
    #also add oversubscribed case
    points.add(nproc * 2)
    return sorted(points)

def is_sudo():
    return os.geteuid() == 0

OVERCOMMIT_FILE = '/proc/sys/vm/overcommit_memory'
HUGEPAGES_FILE = '/sys/kernel/mm/transparent_hugepage/enabled'
OS_CONFIG_STATES = {}

def read_current_overcommit_setting():
    try:
        with open(OVERCOMMIT_FILE, 'r') as f:
            OS_CONFIG_STATES['overcommit'] = f.read().strip()
    except IOError as e:
        print(f"Error reading overcommit: {e}")
        OS_CONFIG_STATES['overcommit'] = None

def read_current_hugepage_setting():
    try:
        with open(HUGEPAGES_FILE, 'r') as f:
            thp_content = f.read().strip()
            match = re.search(r'\[(.*?)\]', thp_content)
            if match:
                OS_CONFIG_STATES['hugepages'] = match.group(1)
            else:
                print(f"Error reading huge pages file: {e}")
                OS_CONFIG_STATES['hugepages'] = None
    except IOError as e:
        print(f"Error reading huge pages file: {e}")
        OS_CONFIG_STATES['hugepages'] = None

def write_os_config_states(settings):
    if settings.get('overcommit'):
        try:
            with open(OVERCOMMIT_FILE, 'w') as f:
                f.write(settings['overcommit'] + '\n')
        except IOError as e:
            print(f"Failed to restore overcommit: {e}")

    if settings.get('hugepages'):
        try:
            with open(HUGEPAGES_FILE, 'w') as f:
                f.write(settings['hugepages'] + '\n')
        except IOError as e:
            print(f"Failed to restore huge pages file: {e}")

def restore_os_config_states():
    settings = OS_CONFIG_STATES
    write_os_config_states(settings)

def prepare_scalloc(enable: bool):
    if(not is_sudo()):
        print("scalloc requires sudo\n")
        exit(-1)

    if not enable:
        #restore settings
        restore_os_config_states()
    else:
        #read current settings
        read_current_overcommit_setting()
        read_current_hugepage_setting()
        #change settings for scalloc
        settings = {"overcommit": "1", "hugepages": "never"}
        write_os_config_states(settings)

def prepare_hugepages(enable: bool, option: str=None):
    if(not is_sudo()):
        print("disabling/enabling hugepages requires sudo\n")
        exit(-1)
    possible_configs = "always never madvise"
    if option != None and option not in possible_configs.split(' '):
        print(f"{option} is not a possible option, use one of: {possible_configs}")
        exit(-1)

    if not enable:
        #restore settings
        restore_os_config_states()
    else:
        #read current settings
        read_current_hugepage_setting()
        settings = {"hugepages": option}
        write_os_config_states(settings)

def start_temperature_monitor(log_file="temperature_log.csv", interval_seconds=1):
    bash_script = f"""
    echo "Timestamp,Temp_mC,Freq_kHz" > {log_file}
    while true; do
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        TEMP=$(cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo "N/A")
        FREQ=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo "N/A")
        echo "$TIMESTAMP,$TEMP,$FREQ" >> {log_file}
        sleep {interval_seconds}
    done
    """
    return subprocess.Popen(['bash', '-c', bash_script])

def stop_temperature_monitor(process):
    process.terminate()

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
        #"mimalloc-batchit",
        "jemalloc:numa:df",
        "snmalloc",
        "hoard:numa",
        "tcmalloc::df",
        "tbbmalloc::df",
        "lockfree:numa:df",
        "rpmalloc::df",
        #"scalloc", #TODO check whether to use df or numa
    ])

    rideables: List[str] = field(default_factory=lambda: [
        "btree_lck",
        "hash_block_lck",
        "leaftree_lck",
        "skiplist_lck",
        "arttree_lck",
        "list_lck",
    ])

    #default sizes for experiments other than varying size
    default_rideables_sizes: Dict[str, int] = field(default_factory=lambda: dict(
        btree_lck =      200_000,
        hash_block_lck = 200_000,
        leaftree_lck =   200_000,
        skiplist_lck =   200_000,
        arttree_lck =    200_000,
        list_lck =         2_000,
    ))

    default_update_perc: int = 100

    #for varying updates experiment
    update_percs: List[int] = field(default_factory=lambda: [1, 5, 50, 90, 100])

    thread_percs: List[int] = field(default_factory=lambda: ["50;50", "75;25", "100;0"])

    #for varying size experiment
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
        ("leaftree_lck",   20_000_000),
        ("skiplist_lck",   20_000_000),
        ("arttree_lck",    20_000_000),
        ("list_lck",           10_000),
        # large
        ("btree_lck",      200_000_000),
        ("hash_block_lck", 200_000_000),
        ("leaftree_lck",   200_000_000),
        ("skiplist_lck",   200_000_000),
        ("arttree_lck",    200_000_000),
        ("list_lck",            10_000),
    ])

    thread_counts: List[int] = field(default_factory=lambda:
        get_numa_aware_thread_sequence(os.cpu_count(), get_num_numa_nodes())
    )

    runs: int = 5
    trial_time_sec: int = 5
    nproc: int = field(default_factory=os.cpu_count)

    hugepages: str = None

    args: argparse.ArgumentParser = None

    def __post_init__(self):
        self.runs = self.args.runs
        self.trial_time_sec = self.args.time
        self.hugepages = self.args.hugepages


@dataclass
class SetbenchConfig:
    """Configuration for setbench ubench allocator+tracker benchmarks."""
    allocators_raw: List[str] = field(default_factory=lambda: [
        "deqalloc",
        "mimalloc",
        #"mimalloc-batchit",
        "jemalloc:numa:df",
        "snmalloc",
        "hoard:numa",
        "tcmalloc::df",
        "tbbmalloc::df",
        "lockfree:numa:df",
        "rpmalloc::df",
        #"scalloc", #TODO check whether to use df or numa
    ])

    rideables: List[str] = field(default_factory=lambda: [
        "guerraoui_ext_bst_ticket",
        "brown_ext_abtree_lf",
        "hm_hashtable",
        "hmlist",
    ])

    #default sizes for experiments other than varying size
    default_rideables_sizes: Dict[str, int] = field(default_factory=lambda: dict(
        guerraoui_ext_bst_ticket = 200_000,
        brown_ext_abtree_lf =      200_000,
        hm_hashtable =             200_000,
        hmlist =                   2_000,
    ))

    trackers: List[str] = field(default_factory=lambda: [
        "2geibr", "debra", "he", "ibr_hp", "ibr_rcu",
        "nbrplus", "qsbr", "wfe", "token4",
    ])

    default_update_perc: int = 100

    default_tracker: str = "debra"
 
    #for varying updates experiment
    update_percs: List[int] = field(default_factory=lambda: [1, 5, 50, 90, 100])

    #for varying size experiment
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
        ("hm_hashtable",               20_000_000),
        ("hmlist",                         10_000),
        # large
        ("guerraoui_ext_bst_ticket",   200_000_000),
        ("brown_ext_abtree_lf",        200_000_000),
        ("hm_hashtable",               100_000_000),
        ("hmlist",                          20_000),
    ])

    thread_counts: List[int] = field(default_factory=lambda:
        get_numa_aware_thread_sequence(os.cpu_count(), get_num_numa_nodes())
    )

    runs: int = 5
    trial_time_ms: int = 5000
    nproc: int = field(default_factory=os.cpu_count)

    hugepages: str = None

    args: argparse.ArgumentParser = None

    def __post_init__(self):
        self.runs = self.args.runs
        self.ttrial_time_secrial_time_sec = self.args.time * 1000
        if self.args.hugepages:
            self.hugepages = self.args.hugepages
        if self.args.tracker:
            #run only this tracker for tracker benchmarks
            self.trackers = [self.args.tracker]
        #change default tracker
        if self.args.default_tracker:
            self.default_tracker = self.args.default_tracker
        

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
    if os.path.isfile(lib):
        return lib
    if os.path.islink(lib) and os.path.exists(lib):
        return lib
    return None



# ---------------------------------------------------------------------------
# Output / results file helpers
# ---------------------------------------------------------------------------

class ResultsFile:
    def __init__(self, path: str, alloc_dir: str, experiment: str):
        today = datetime.now().strftime("%m-%d-%Y")
        self.f = open(path, "a")
        self.f.write(f"Command: {' '.join(sys.argv)}\n")
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

        try:
            allocator_versions = open(f"{alloc_dir}/versions.txt", "r")
            self.f.write(f"allocator versions:\n")
            for line in allocator_versions.readlines():
                line = line.strip()
                alloc, version = line.split(' ')
                self.f.write(f"\t{alloc}: {version}\n")
        except Exception:
            pass
        self.f.write("---\n")
        self.f.flush()

    def write(self, line: str):
        self.f.write(line)
        self.f.flush()
        print(line, end="", flush=True)

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

        self.fmt = "{:<21} {:<7} {:<20} {:<10} {:<9} {:<6} {:<21} {}"
        self.header = self.fmt.format("allocator", "update%", "ds", "key_size", "#threads", "numa", "thread-perc", "results")
        self.path = self._output_path("flock")

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

    #run one experiment accross all allocators
    def run_all_allocs(self, rideable, update_perc=None, size=None, nthreads=None, add_args=""):
        if size is None:
            size = self.config.default_rideables_sizes.get(rideable)
        if update_perc is None:
            update_perc = self.config.default_update_perc
        if nthreads is None:
            nthreads = self.config.nproc

        cfg = self.config
        for raw_alloc in cfg.allocators_raw:
            alloc, use_numa, _ = parse_allocator(raw_alloc)
            lib = find_allocator_lib(self.alloc_dir, alloc)
            if lib is None:
                print(f"  WARNING: allocator lib not found: {alloc}", file=sys.stderr)
                continue

            numa_cmd = "numactl -i all" if use_numa else ""
            prefix = self.fmt.format(alloc, update_perc, rideable, size, nthreads, str(use_numa), add_args, "[ ")
            self.rf.write(prefix)

            binary = self._binary(rideable)
            if binary is None:
                print(f"  WARNING: binary not found: {binary}", file=sys.stderr)
                continue

            # flock binary accepts -r for multiple runs internally
            half_n = size // 2
            cmd = (
                f"PARLAY_NUM_THREADS={cfg.nproc} "
                f"LD_PRELOAD={lib} "
                f'/usr/bin/time -f "%M KiloBytes" '
                f"{numa_cmd} "
                f"{binary} "
                f"-p {nthreads} -u {update_perc} -n {half_n} "
                f"-t {cfg.trial_time_sec} -r {cfg.runs} "
                f"{add_args} "
            ).strip()

            if alloc == "scalloc": prepare_scalloc(True)
            if self.config.hugepages: prepare_hugepages(True, self.config.hugepages)

            output, status = run_command(cmd)

            if self.config.hugepages: prepare_hugepages(False)
            if alloc == "scalloc": prepare_scalloc(False)

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
            suffix = f"{tps_str} ] {tp_avg:.4f}, {mem_str}\n"
            self.rf.write(suffix)

            if status != "ok":
                self.rf.write(f"# {status.upper()}: {rideable} alloc={alloc} u={update_perc} n={size}\n")

    def new_results_file(self, name: str):
        print(f"Starting {name} experiment")
        self.rf = ResultsFile(f"{self.output_dir}/{name}", self.alloc_dir, f"flock {name}")
        self.rf.write(self.header + "\n")

    #vary update%
    def run_updates(self):
        self.new_results_file("updates")
        for rideable in self.config.rideables:
            for update_perc in self.config.update_percs:
                self.run_all_allocs(
                    rideable=rideable,
                    update_perc=update_perc,
                )
        self.rf.close()

    #vary number if threads
    def run_threads(self):
        self.new_results_file("threads")
        for rideable in self.config.rideables:
            for nthreads in self.config.thread_counts:
                self.run_all_allocs(
                    rideable=rideable,
                    nthreads=nthreads
                )
        self.rf.close()

    #vary data structure size
    def run_sizes(self, experiment="sizes"):
        self.new_results_file(experiment)
        for rideable, size in self.config.rideables_sizes:
            self.run_all_allocs(
                rideable=rideable,
                size=size,
            )
        self.rf.close()

    #vary update skewness (some threads might do more deletes, others more inserts)
    def run_thread_perc(self):
        self.new_results_file("thread_perc")
        for rideable in self.config.rideables:
            for thread_perc in self.config.thread_percs:
                self.run_all_allocs(
                    rideable=rideable,
                    add_args=f"-thread-perc \"{thread_perc}\"",
                )
        self.rf.close()

    #do upserts instead of regular insters
    def run_upserts(self):
        self.new_results_file("upserts")
        for rideable in self.config.rideables:
            self.run_all_allocs(
                rideable=rideable,
                add_args=f"-upsert",
            )
        self.rf.close()

    #run with hugepages=never
    def run_hugepages(self):
        prev_hp_setting = self.config.hugepages
        self.config.hugepages = "never"
        self.run_sizes("hugepages")
        self.config.hugepages = prev_hp_setting
        self.rf.close()

    def run_ablation_localseglist(self):
        prev_allocs = self.config.allocators_raw
        self.config.allocators_raw = ["deqalloc", "deqalloc_localseglist"]
        self.run_sizes("ablation_localseglist")
        self.config.allocators_raw = prev_allocs
        self.rf.close()

    def run(self):
        b = self.config.args.benchmark
        run_all = "all" in b
        if run_all or "updates"     in b: self.run_updates()
        if run_all or "threads"     in b: self.run_threads()
        if run_all or "sizes"       in b: self.run_sizes()
        if not self.config.args.nohugepages:
            if run_all or "hugepages"   in b: self.run_hugepages()
        if run_all or "ablation"    in b: self.run_ablation_localseglist()
        if run_all or "thread-perc" in b: self.run_thread_perc()
        if run_all or "upserts"     in b: self.run_upserts()


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

        self.fmt = "{:<21} {:<10} {:<10} {:<25} {:<10} {:<9} {:<6} {}"
        self.header = self.fmt.format("allocator", "update%", "scheme", "ds", "key_size", "#threads", "numa", "results")
        self.path = self._output_path("setbench")

    def _output_path(self, name: str) -> str:
        today   = datetime.now().strftime("%m-%d-%Y")
        machine = get_machine_info()
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
            return os.path.join(self.output_dir, name)
        return f"../../timings/{name}_{self.config.nproc}-{machine}_{today}"

    def _binary(self, rideable: str, tracker: str, df: bool) -> Optional[str]:
        df_suffix = "_df" if df and tracker != "token4" else ""
        name = f"ubench_{rideable}.alloc_new.reclaim_{tracker}{df_suffix}.pool_none.out"
        path = os.path.join(self.bench_dir, name)
        return path if os.path.isfile(path) else None

    #run one experiment accross all allocators
    def run_all_allocs(self, rideable, update_perc=None, size=None, nthreads=None, tracker=None):
        if size is None:
            size = self.config.default_rideables_sizes.get(rideable)
        if update_perc is None:
            update_perc = self.config.default_update_perc
        if nthreads is None:
            nthreads = self.config.nproc
        if tracker is None:
            tracker = self.config.default_tracker

        cfg = self.config
        for raw_alloc in cfg.allocators_raw:
            alloc, use_numa, use_df = parse_allocator(raw_alloc)
            df_suffix = "_df" if use_df else ""
            scheme_label = f"{tracker}{df_suffix}"

            binary = self._binary(rideable, tracker, use_df)
            if binary is None:
                print(f"  WARNING: binary not found: {binary}", file=sys.stderr)
                continue

            lib = find_allocator_lib(self.alloc_dir, alloc)
            if lib is None:
                print(f"  WARNING: allocator lib not found: {alloc}", file=sys.stderr)
                continue

            numa_cmd = "numactl -i all" if use_numa else ""
            update_half = round(update_perc / 2, 2)

            prefix = self.fmt.format(alloc, update_perc, scheme_label,
                                rideable, size, nthreads, str(use_numa), "[ ")
            self.rf.write(prefix)

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
                    f"-nwork {nthreads} -nprefill {nthreads} "
                    f"-i {update_half} -d {update_half} "
                    f"-rq 0 -rqsize 1 -k {size} -nrq 0 "
                    f"-t {cfg.trial_time_ms}"
                ).strip()


                if alloc == "scalloc": prepare_scalloc(True)
                if self.config.hugepages: prepare_hugepages(True, self.config.hugepages)

                output, status = run_command(cmd)

                if self.config.hugepages: prepare_hugepages(False)
                if alloc == "scalloc": prepare_scalloc(False)

                if status != "ok":
                    last_status = status

                # throughput
                m_tp = re.search(r"total throughput\s*[:\s]\s*([\d.]+)", output)
                if m_tp:
                    tp = float(m_tp.group(1)) / 10**6 #show in mops
                    tps.append(tp)
                    tp_avg += tp / cfg.runs

                # memory
                m_mem = re.search(r"(\d+)\s+KiloBytes /usr/bin/time output", output)
                if m_mem:
                    memusage_avg += int(m_mem.group(1)) // cfg.runs

            tps_str = " ".join(str(v) for v in tps)
            suffix  = f"{tps_str} ] {tp_avg:.4f}, {memusage_avg} KB\n"
            self.rf.write(suffix)

            if last_status != "ok":
                self.rf.write(
                    f"# {last_status.upper()}: {rideable} "
                    f"tracker={scheme_label} alloc={alloc} u={update_perc} k={size}\n"
                )

    def new_results_file(self, name: str):
        print(f"Starting {name} experiment")
        self.rf = ResultsFile(f"{self.output_dir}/{name}", self.alloc_dir, f"setbench {name}")
        self.rf.write(self.header + "\n")

    #vary update%
    def run_updates(self):
        self.new_results_file("updates")
        for rideable in self.config.rideables:
            for update_perc in self.config.update_percs:
                self.run_all_allocs(
                    rideable=rideable,
                    update_perc=update_perc,
                )
        self.rf.close()

    #vary number if threads
    def run_threads(self):
        self.new_results_file("threads")
        for rideable in self.config.rideables:
            for nthreads in self.config.thread_counts:
                self.run_all_allocs(
                    rideable=rideable,
                    nthreads=nthreads
                )
        self.rf.close()

    #vary data structure size
    def run_sizes(self, experiment="sizes"):
        self.new_results_file(experiment)
        for rideable, size in self.config.rideables_sizes:
            self.run_all_allocs(
                rideable=rideable,
                size=size,
            )
        self.rf.close()

    #vary reclamation scheme
    def run_trackers(self):
        self.new_results_file("trackers")
        for rideable in self.config.rideables:
            for tracker in self.config.trackers:
                self.run_all_allocs(
                    rideable=rideable,
                    tracker=tracker,
                )
        self.rf.close()

    #run with hugepages=never
    def run_hugepages(self):
        prev_hp_setting = self.config.hugepages
        self.config.hugepages = "never"
        self.run_sizes("hugepages")
        self.config.hugepages = prev_hp_setting
        self.rf.close()

    def run_ablation_localseglist(self):
        prev_allocs = self.config.allocators_raw
        self.config.allocators_raw = ["deqalloc", "deqalloc_localseglist"]
        self.run_sizes("ablation_localseglist")
        self.config.allocators_raw = prev_allocs
        self.rf.close()

    def run(self):
        b = self.config.args.benchmark
        run_all = "all" in b
        if run_all or "trackers"  in b: self.run_trackers()
        if run_all or "updates"   in b: self.run_updates()
        if run_all or "threads"   in b: self.run_threads()
        if run_all or "sizes"     in b: self.run_sizes()
        if not self.config.args.nohugepages:
            if run_all or "hugepages" in b: self.run_hugepages()
        if run_all or "ablation"  in b: self.run_ablation_localseglist()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    #make sure we are in the right directory
    benchmark_dir=subprocess.getoutput("realpath "+re.sub(os.path.basename(__file__), "", os.path.realpath(__file__)))+"/"
    curr_dir = os.getcwd()
    os.chdir(benchmark_dir) #pushd

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
                        default=os.path.join(script_dir, "../build/benchmarks/setbench"),
                        help="Path to setbench benchmark binaries")
    parser.add_argument("--alloc-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/allocators"),
                        help="Path to allocator .so files for flock")
    parser.add_argument("--runs",        type=int, default=5,       help="Number of runs (default: 5)")
    parser.add_argument("--allocator",  default=["all"], nargs="+", help="Run only specific allocator(s)")
    parser.add_argument("--ds",          default=None,              help="Run only this data structure")
    parser.add_argument("--default-tracker", default="debra",       help="Run benchmarks with this tracker/memory reclamation scheme.")
    parser.add_argument("--tracker",     default=None,              help="Run only this tracker/memory reclamation scheme for tracker benchmarks.")
    parser.add_argument("--time",        type=int, default=5,       help="Amount of time each run takes (default: 5)")
    parser.add_argument("--benchmark",   type=str, default=["all"], help="Run specific benchmark(s) (default: all)",
                            nargs="+", choices=["updates", "sizes", "threads", "trackers", "thread-perc", "upserts", "all"])
    parser.add_argument("--hugepages",   default=None,              help="Set hugepages setting",
                        choices=["never", "always", "madvise"])
    parser.add_argument("--nohugepages", action='store_true',       help="Do not run hugepages benchmark")

    args = parser.parse_args()

    if not args.nohugepages and not is_sudo():
        print("disabling/enabling hugepages requires sudo")
        print("either run with sudo or add --nohugepages\n")
        exit(-1)

    # -- flock config --
    flock_cfg = FlockConfig(args=args)
    if args.allocator:
        flock_cfg.allocators_raw = [a for a in flock_cfg.allocators_raw
                                    if a.split(":")[0] in args.allocator or args.allocator == ["all"]]
    if args.ds:
        flock_cfg.rideables_sizes = [(r, s) for r, s in flock_cfg.rideables_sizes if r == args.ds]

    # -- setbench config --
    sb_cfg = SetbenchConfig(args=args)
    if args.allocator:
        sb_cfg.allocators_raw = [a for a in sb_cfg.allocators_raw
                                 if a.split(":")[0] in args.allocator or args.allocator == ["all"]]
    if args.ds:
        sb_cfg.rideables_sizes = [(r, s) for r, s in sb_cfg.rideables_sizes if r == args.ds]

    if not is_sudo() and \
       ("scalloc" in flock_cfg.allocators_raw or \
       "scalloc" in sb_cfg.allocators_raw):
        print("scalloc requires sudo\n")
        exit(-1)

    flock_runner    = FlockRunner(flock_cfg,  args.flock_dir,    args.alloc_dir, f"{args.output}/flock")
    setbench_runner = SetbenchRunner(sb_cfg,  args.setbench_dir, args.alloc_dir, f"{args.output}/setbench")

    experiments = {
        "flock":    [(flock_runner.run,    "Flock Allocator Benchmarks")],
        "setbench": [(setbench_runner.run, "Setbench Allocator+Tracker Benchmarks")],
        "all":      [(setbench_runner.run, "Setbench Allocator+Tracker Benchmarks"),
                     (flock_runner.run,    "Flock Allocator Benchmarks")],
    }

    temp_daemon = start_temperature_monitor(f"{args.output}/temperature.csv", 5)

    for func, label in experiments[args.experiment]:
        print(f"\n{'='*55}")
        print(f"  {label}")
        print(f"{'='*55}")
        start = time.time()
        func()
        elapsed = time.time() - start
        print(f"Completed in {elapsed:.1f}s ({elapsed/60:.1f}m)")

    stop_temperature_monitor(temp_daemon)

    os.chdir(curr_dir) #popd

if __name__ == "__main__":
    main()
