#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import re
import glob
import shutil
import csv
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple, Optional

# ---------------------------------------------------------------------------
# Helpers
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
            # Uncomment the next line if you want to see the real-time output in the console
            # print(line, end="")
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

def parse_allocator(raw: str) -> Tuple[str, bool]:
    """Parse 'name[:numa]' → (name, use_numa)."""
    parts = raw.split(":")
    name     = parts[0]
    use_numa = len(parts) > 1 and parts[1] == "numa"
    return name, use_numa

def parse_perf_output(file_path):
    line_pattern = re.compile(r'^\s+(\d+\.\d+%)\s+(\d+\.\d+%)\s+')
    perf_data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = line_pattern.match(line)
                if match:
                    children_pct = match.group(1)
                    self_pct = match.group(2)
                    symbol_start = line.find('[.]')
                    if symbol_start == -1:
                        symbol_start = line.find('[k]')
                    if symbol_start != -1:
                        symbol = line[symbol_start + 4:].strip()
                    else:
                        parts = line.split()
                        symbol = " ".join(parts[4:]) if len(parts) > 4 else "Unknown"
                    perf_data[symbol] = {
                        'children': children_pct,
                        'self': self_pct
                    }
        return perf_data
    except FileNotFoundError:
        print(f"Error: Could not find file '{file_path}'")
        return {}

def append_to_csv(file_path, row_data):
    fieldnames = [
        'allocator', 
        'update%', 
        'ds', 
        'tracker',
        'key_size', 
        '#threads', 
        'Numa', 
        'remote freeing (%)'
    ]
    file_exists = os.path.isfile(file_path) and os.path.getsize(file_path) > 0
    with open(file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_data)

#get percentage of time spent remote freeing
def get_remote_freeing_perc(results, allocator):
    if "jemalloc" in allocator:
        return results["je_tcache_bin_flush_small"]["children"]
    elif "mimalloc" in allocator:
        return results["mi_free_block_delayed_mt"]["children"]
    else:
        assert(False)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class PerfConfig:
    """Configuration for perf allocator+tracker benchmarks."""
    allocators_raw: List[str] = field(default_factory=lambda: [
        "mimalloc", 
        "jemalloc:numa"
    ])
    
    trackers: List[str] = field(default_factory=lambda: [
        "debra", "debra_df", "2geibr", "2geibr_df", "he", "he_df", 
        "ibr_hp", "ibr_hp_df", "ibr_rcu", "ibr_rcu_df", "nbr", 
        "nbr_df", "nbrplus", "nbrplus_df", "qsbr", "qsbr_df", 
        "token4", "wfe", "wfe_df"
    ])

    rideables_sizes: List[Tuple[str, int]] = field(default_factory=lambda: [
        ("guerraoui_ext_bst_ticket", 200000),
        ("brown_ext_abtree_lf", 200000),
        ("hm_hashtable", 200000),
        ("hmlist", 2000)
    ])

    update_percs: List[int] = field(default_factory=lambda: [100])
    
    runs: int = 1
    trial_time_ms: int = 5000
    nproc: int = field(default_factory=lambda: os.cpu_count() or 1)
    
    args: argparse.ArgumentParser = None

    def __post_init__(self):
        if self.args:
            self.runs = self.args.runs
            self.trial_time_ms = self.args.time * 1000
            if self.args.allocator and "all" not in self.args.allocator:
                self.allocators_raw = self.args.allocator

# ---------------------------------------------------------------------------
# Perf Experiment Runner
# ---------------------------------------------------------------------------

class PerfRunner:
    def __init__(self, config: PerfConfig, script_dir: str, binary_dir: str, alloc_dir: str, output_dir: str):
        self.config     = config
        self.script_dir = script_dir
        self.binary_dir  = binary_dir
        self.alloc_dir  = alloc_dir
        self.output_dir = output_dir

    def _binary(self, rideable: str, tracker: str) -> Optional[str]:
        name = f"ubench_{rideable}.alloc_new.reclaim_{tracker}.pool_none.out"
        path = os.path.join(self.binary_dir, name)
        return path if os.path.isfile(path) else path # Return path anyway so crash handler catches missing files

    def run_perf_collection(self):
        cfg = self.config
        
        for rideable, size in cfg.rideables_sizes:
            for tracker in cfg.trackers:
                for update_perc in cfg.update_percs:
                    update_half = int(update_perc / 2)
                    
                    for raw_alloc in cfg.allocators_raw:
                        alloc, use_numa = parse_allocator(raw_alloc)
                        
                        numa_cmd = "numactl -i all" if use_numa else ""
                        numa_name = "_numa" if use_numa else ""
                        
                        lib = os.path.join(self.alloc_dir, f"lib{alloc}.so")
                        binary = self._binary(rideable, tracker)

                        for a in range(1, cfg.runs + 1):
                            perf_file = f"perf_{alloc}{numa_name}_{rideable}:{size}_{tracker}_{update_perc}u_{a}.data"
                            report_file = f"{perf_file}.txt"

                            print(f"\n--- [Recording] {alloc}{numa_name} | {rideable} | {tracker} | Run {a} ---")
                            
                            # 1. Run Perf Record
                            record_cmd = (
                                f"NO_DESTRUCT=1 LD_PRELOAD={lib} "
                                f"{numa_cmd} perf record -g -o {perf_file} "
                                f"{binary} -nwork {cfg.nproc} -nprefill {cfg.nproc} "
                                f"-i {update_half} -d {update_half} -rq 0 -rqsize 1 -k {size} "
                                f"-nrq 0 -t {cfg.trial_time_ms}"
                            ).strip()

                            _, status = run_command(record_cmd, timeout_sec=(cfg.trial_time_ms // 1000) + 10)

                            # 2. Generate Perf Report (if recording succeeded)
                            if status == "ok" and os.path.exists(perf_file):
                                print(f"  -> Generating report: {report_file}")
                                report_cmd = f"perf report -i {perf_file} --stdio --percent-limit 0.01 > {report_file}"
                                _, status = run_command(report_cmd)
                            else:
                                print(f"  -> Skipping report generation due to previous error/timeout.")
                                exit(-1)

                            if status == "ok" and os.path.exists(report_file):
                                d = parse_perf_output(report_file)
                            else:
                                print(f"  -> Skipping result processing due to previous error/timeout.")
                                exit(-1)

                            run = {
                                'allocator': alloc,
                                'update%': update_perc,
                                'ds': rideable,
                                'tracker': tracker,
                                'key_size': size,
                                '#threads': cfg.nproc,
                                'Numa': use_numa,
                                'remote freeing (%)': get_remote_freeing_perc(d, alloc),
                            }

                            append_to_csv("results.csv", run)

                            # 3. Cleanup raw data
                            if os.path.exists(perf_file):
                                os.remove(perf_file)

    def process_results(self):
        print("\n================ Processing Results ================")
        cfg = self.config
        
        for raw_alloc in cfg.allocators_raw:
            alloc, use_numa = parse_allocator(raw_alloc)
            numa_name = "_numa" if use_numa else ""

            glob_pattern = f"perf_{alloc}{numa_name}_*.data.txt"
            files_to_merge = glob.glob(glob_pattern)

            if not files_to_merge:
                print(f"No files found matching {glob_pattern} for merge.")
                continue

            # 1. Merge Text
            merge_script = os.path.join(self.script_dir, "merge_perf_txt.py")
            merge_cmd = f"python3 {merge_script} " + " ".join(files_to_merge)
            merge_out_name = f"perf_merge_{alloc}{numa_name}.merge.txt"
            
            print(f"--- Merging: {alloc}{numa_name} ---")
            output, status = run_command(merge_cmd)
            if status == "ok":
                print(output)
                with open(merge_out_name, "w") as f:
                    f.write(output)

            # 2. Plotting
            plot_script = os.path.join(self.script_dir, "perf_plot.py")
            plot_cmd = f"python3 {plot_script} " + " ".join(files_to_merge)
            print(f"--- Plotting: {alloc}{numa_name} ---")
            run_command(plot_cmd)

            # 3. File Renaming
            if os.path.exists("perf_plot.png"):
                shutil.move("perf_plot.png", f"perf_{alloc}{numa_name}.png")
            if os.path.exists("perf_plot.pdf"):
                shutil.move("perf_plot.pdf", f"perf_{alloc}{numa_name}.pdf")
            if os.path.exists("allocator_data.csv"):
                shutil.move("allocator_data.csv", f"{alloc}{numa_name}_data.csv")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description="Perf Allocator Experiment Runner")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs (default: 1)")
    parser.add_argument("--time", type=int, default=3, help="Amount of time each run takes in seconds (default: 3)")
    parser.add_argument("--allocator", default=["all"], nargs="+", help="Run only specific allocator(s) e.g., mimalloc jemalloc:numa")
    parser.add_argument("--alloc-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/allocators"),
                        help="Path to allocator .so files")
    parser.add_argument("--binary-dir", metavar="DIR",
                        default=os.path.join(script_dir, "../build/benchmarks/setbench"),
                        help="Path to setbench binaries")
    args = parser.parse_args()

    # Pre-flight check: Build allocators if they don't exist
    if not os.path.isdir(args.alloc_dir):
        print("Allocator directory not found, specify --alloc-dir.")
        exit(-1)

    # Setup directories
    d = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    results_dir = os.path.join(script_dir, "perf_results", d)
    os.makedirs(results_dir, exist_ok=True)
    
    # Emulate `pushd`
    os.chdir(results_dir)

    # Initialize and run
    config = PerfConfig(args=args)
    runner = PerfRunner(config=config,
        script_dir=script_dir,
        alloc_dir=args.alloc_dir,
        binary_dir=args.binary_dir,
        output_dir=results_dir)
    
    runner.run_perf_collection()
    runner.process_results()
    
    # Emulate `popd`
    os.chdir(script_dir)
    print("\nBenchmarking complete!")

if __name__ == "__main__":
    main()