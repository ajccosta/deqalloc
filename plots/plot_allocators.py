#!/usr/bin/env python3

import sys
import re
import math
import os
import os.path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.colors import LinearSegmentedColormap
from collections import defaultdict
import argparse
import statistics as stat
from matplotlib.patches import Patch
import matplotlib as mpl
import pandas as pd
import io

pdfmerge = True
try:
    import fitz
except ImportError:
   print("ERROR: import fitz failed, not merging pdfs (pip install pymupdf)\n")
   pdfmerge = False

# -- Aesthetics --------------------------------------------------------------
DARK_BG   = "#0e1117"
PANEL_BG  = "#161b25"
GRID_COL  = "#242c3a"
TEXT_COL  = "#e8edf5"
ACCENT    = "#4fc3f7"

ALLOC_PALETTE = {
    "deqalloc":  "#251cc5",
    "mimalloc":  "#81c784",
    "jemalloc":  "#ffb74d",
    "snmalloc":  "#ce93d8",
    "hoard":     "#f48fb1",
    "tcmalloc":  "#ef5350",
    "tbbmalloc": "#ff7043",
    "lockfree":  "#26c6da",
    "rpmalloc":  "#d4e157",
}

ALLOCS = list(ALLOC_PALETTE.keys())

DS_LABELS = {
    "btree_lck"                 : "b-tree",
    "hash_block_lck"            : "hash-block",
    "leaftree_lck"              : "leaf-tree",
    "skiplist_lck"              : "skip-list",
    "arttree_lck"               : "art-tree",
    "list_lck"                  : "linked-list",
    "guerraoui_ext_bst_ticket"  : "bst-tk",
    "brown_ext_abtree_lf"       : "abtree",
    "hmlist"                    : "hmlist",
    "hm_hashtable"              : "hmhash",
}

TRACKER_LABELS = {
    "2geibr"    : "ibr",
    "debra"     : "debra",
    "he"        : "he",
    "ibr_hp"    : "hp",
    "ibr_rcu"   : "ebr",
    "nbr"       : "nbr",
    "nbrplus"   : "nbr+",
    "qsbr"      : "qsbr",
    "wfe"       : "wfe",
    "token4"       : "token",
    "2geibr_df"    : "ibr",
    "debra_df"     : "debra",
    "he_df"        : "he",
    "ibr_hp_df"    : "hp",
    "ibr_rcu_df"   : "ebr",
    "nbr_df"       : "nbr",
    "nbrplus_df"   : "nbr+",
    "qsbr_df"      : "qsbr",
    "wfe_df"       : "wfe",
    "token4_df"    : "token",
}

DS_TYPES = {
    "btree_lck"                 : "normal",
    "hash_block_lck"            : "normal",
    "leaftree_lck"              : "normal",
    "skiplist_lck"              : "normal",
    "arttree_lck"               : "normal",
    "list_lck"                  : "list"  ,
    "guerraoui_ext_bst_ticket"  : "normal",
    "brown_ext_abtree_lf"       : "normal",
    "hmlist"                    : "list"  ,
    "hm_hashtable"              : "normal",
}

ALLOC_MARKERS = {
    'deqalloc': 's',
    'mimalloc': 'o',
    'jemalloc': 'P',
    'snmalloc': 'h',
    'hoard': 'H',
    'tcmalloc': 'd',
    'tbbmalloc': 'p',
    'lockfree': 'v',
    'rpmalloc': '<',
    #'': '>',
    #'': '8',
    #'': 'h',
    #'': 'H',
    #'': '^',
    #'': 'd',
    #'': 'v',
    #'': 'v',
    #'': '<',
    #'': 'X',
}

#order in which lines appear
ALLOC_ZORDER = {
    "deqalloc":  8,
    "mimalloc":  7,
    "jemalloc":  6,
    "snmalloc":  5,
    "hoard":     4,
    "tcmalloc":  3,
    "tbbmalloc": 2,
    "lockfree":  1,
    "rpmalloc":  0,
}

ALLOC_HATCHES = {
    "deqalloc":  "///",
    "mimalloc":  "\\\\\\",
    "jemalloc":  "|||",
    "snmalloc":  "---",
    "hoard":     "+++",
    "tcmalloc":  "xxx",
    "tbbmalloc": "ooo",
    "lockfree":  "OOO",
    "rpmalloc":  "...",
}

scale = 1.5

FIG_CONFIGS = {
    "figsize": (2.5, 1.5),
    "linewidth": 1.4,
    "markersize": 3,
    "xlabel_fontsize": scale * 7.5,
    "ylabel_fontsize": scale * 6.0,
    "xtick_fontsize":  scale * 6.5,
    "ytick_fontsize":  scale * 6.5,
    "legend_fontsize": scale * 4.5,
    "title_fontsize":  scale * 8,
    "legend_ncols": len(ALLOC_PALETTE)/3,
    "dpi": 300,
    "pad_inches": 0.015,
    "xtick_end_margin": 0.1,
    "bar_linewidth": 0.7,
    "linestyle": 'dashed',
}

#which data structures/trackers to show for the paper for the varying plots
PAPER_DS_FLOCK = ["skiplist_lck", "leaftree_lck", "hash_block_lck"]

#PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hm_hashtable", "hmlist"]
PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hm_hashtable"]
PAPER_TRACKERS_SETBENCH = ["ibr", "debra", "he", "hp", "ebr", "nbr+", "qsbr", "wfe"]
PAPER_HUGEPAGES_ALLOCS_SETBENCH = ["deqalloc", "hoard", "mimalloc", "snmalloc"]

SUITES = ["flock", "setbench"]

mpl.rcParams["hatch.linewidth"] = FIG_CONFIGS.get("bar_linewidth")

def style_fig(fig, ax, paper_print=True):
    ax.tick_params(axis='x', labelsize=FIG_CONFIGS["xtick_fontsize"])
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"])

    ylabel = ax.yaxis.label
    xlabel = ax.xaxis.label
    xlabel.set_fontsize(FIG_CONFIGS["xlabel_fontsize"])
    ylabel.set_fontsize(FIG_CONFIGS["ylabel_fontsize"])

    ax.title.set_fontsize(fontsize=FIG_CONFIGS["title_fontsize"])
    ax.title.set_fontweight('bold')

    fig.patch.set_edgecolor('none')

    ax.set_ylim(bottom=0)
    ax.grid(linestyle='--')

    if not paper_print and ax.get_legend() is not None:
        ax.legend(
            bbox_to_anchor=(0.5, -0.5),
            frameon=True,
            fontsize=FIG_CONFIGS["legend_fontsize"],
            ncols=FIG_CONFIGS["legend_ncols"],
            loc="center",
            alignment="center"
        )

    else:
        #plt.tight_layout()
        pass

# -- Parser -------------------------------------------------------------------
def parse_flock(path):
    rows = []
    crashes = []
    crash_re = re.compile(r"#\s*CRASH:\s*(\w+)\s+alloc=(\w+)\s+u=(\d+)\s+n=(\d+)")
    row_re   = re.compile(
        r"^([\w-]+)\s+(\d+)\s+(\w+)\s+(\d+)\s+(\d+)\s+(True|False)\s+(.*?)\s+\[([^\]]*)\]\s+([\d.]+),\s*([\d.]+)\s*KB"
    )
    with open(path) as f:
        for line in f:
            line = line.strip()
            m = crash_re.match(line)
            if m:
                crashes.append(dict(ds=m.group(1), allocator=m.group(2),
                                    update=int(m.group(3)), key_size=int(m.group(4))))
                continue
            m = row_re.match(line)
            if m:
                vals_str = m.group(8).strip()
                vals = [float(x) for x in vals_str.split()] if vals_str else []
                mean = stat.mean(vals) if len(vals) > 0 else 0
                gmean = stat.geometric_mean(vals) if len(vals) > 0 else 0
                entry = dict(
                    allocator=m.group(1),
                    update=int(m.group(2)),
                    ds=m.group(3),
                    key_size=int(m.group(4)),
                    threads=int(m.group(5)),
                    numa=m.group(6) == "True",
                    thread_flags=m.group(7).strip(),
                    values=vals,
                    mean=mean,
                    gmean=gmean,
                    mem_kb=float(m.group(10)),
                    reclamation="debra", #hacky way to integrate with other suites
                )
                rows.append(entry)
                if abs(mean - gmean) > 0.5 * 10**1:
                    print("Reasonable difference in gmean", entry)
                if abs(mean - float(m.group(9))) > 10**-3:
                    print(f"Error in mean checksum. Given: {float(m.group(9))}, Calculated: {mean}")
    return rows, crashes

def parse_setbench(path):
    rows = []
    crashes = []
    crash_re = re.compile(r"#\s*CRASH:\s*(\w+)\s+alloc=(\w+)\s+u=(\d+)\s+n=(\d+)")
    row_re   = re.compile(
        r"^(\w+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(\d+)\s+(True|False)\s+\[([^\]]*)\]\s+([\d.]+),\s*([\d.]+)\s*KB"
    )
    with open(path) as f:
        for line in f:
            line = line.strip()
            m = crash_re.match(line)
            if m:
                crashes.append(dict(ds=m.group(1), allocator=m.group(2),
                                    update=int(m.group(3)), key_size=int(m.group(4))))
                continue
            m = row_re.match(line)
            if m:
                vals_str = m.group(8).strip()
                vals = [float(x) for x in vals_str.split()] if vals_str else []
                mean = stat.mean(vals) if len(vals) > 0 else 0
                gmean = stat.geometric_mean(vals) if len(vals) > 0 else 0
                
                entry = dict(
                    allocator=m.group(1),
                    update=int(m.group(2)),
                    reclamation=TRACKER_LABELS.get(m.group(3)),
                    ds=m.group(4),
                    key_size=int(m.group(5)),
                    threads=int(m.group(6)),
                    numa=m.group(7) == "True",
                    values=vals,
                    mean=mean,
                    gmean=gmean,
                    mem_kb=float(m.group(10)),
                )
                rows.append(entry)
                if abs(mean - gmean) > 0.5 * 10**1:
                    print("Reasonable difference in gmean", entry)
                if abs(mean - float(m.group(9))) > 10**-3:
                    print(f"Error in mean checksum. Given: {float(m.group(9))}, Calculated: {mean}")
    return rows, crashes

# -- Helpers ------------------------------------------------------------------
def group_by(rows, *keys):
    d = defaultdict(list)
    for r in rows:
        k = tuple(r[k] for k in keys)
        d[k].append(r)
    return d

def fmt_size(n):
    if n >= 1_000_000: return f"{n//1_000_000}M"
    if n >= 1_000:     return f"{n//1_000}K"
    return str(n)

#get nice scientific notation label
def get_nice_scinot_labels(x_vals):
    labels = []
    for x in x_vals:
        exp = int(np.floor(np.log10(x)))
        mant = x / 10**exp
        labels.append(f"${mant:.0f}\\!\\!\\times\\!\\!10^{{{exp}}}$")
    return labels

def merge_pdfs_horizontally(pdf_list, output_path):
    if pdf_list == []:
        return
    if not pdfmerge: #package not imported
        return
    docs = [fitz.open(pdf) for pdf in pdf_list]
    pages = [doc[0] for doc in docs]
    total_width = sum(page.rect.width for page in pages)
    max_height = max(page.rect.height for page in pages)
    out_doc = fitz.open()
    out_page = out_doc.new_page(width=total_width, height=max_height)
    current_x = 0
    for i, page in enumerate(pages):
        rect = fitz.Rect(current_x, 0, current_x + page.rect.width, page.rect.height)
        out_page.show_pdf_page(rect, docs[i], 0)
        current_x += page.rect.width
    out_doc.save(output_path)
    for doc in docs:
        doc.close()
    out_doc.close()
    print(f"merged {len(pdf_list)} pdfs to {output_path}")

def which_paper_ds(dss):
    paper_ds = []
    if set(dss).intersection(set(PAPER_DS_FLOCK)):
        paper_ds = PAPER_DS_FLOCK
    if set(dss).intersection(set(PAPER_DS_SETBENCH)):
        assert(paper_ds == [])
        paper_ds = PAPER_DS_SETBENCH
    #assert(paper_ds != [])
    return paper_ds

def merge_entries(data):
    if len(data) == 1:
        return data[0]
    if len(data) == 0:
        return {}
    merged_data = {}
    config_keys = ['allocator', 'update', 'reclamation', 'ds', 'key_size', 'threads', 'numa']
    #check that config_keys are equal
    for k in config_keys:
        conf = data[0][k]
        merged_data[k] = conf
        for i in range(1, len(data)):
            if(not(data[i][k] == conf)):
                print(data[i][k], conf)
            assert(data[i][k] == conf)
    new_values = []
    new_mem_kb = 0
    for d in data:
        new_values.extend(d["values"].copy())
        new_mem_kb += d["mem_kb"] / len(data)
    merged_data["values"] = new_values
    merged_data["mean"] = stat.mean(new_values)
    merged_data["gmean"] = stat.geometric_mean(new_values)
    merged_data["mem_kb"] = new_mem_kb
    return merged_data


def load_file(input_dir, suite, experiment):
    if suite == "flock":
        parse_f = parse_flock
    elif suite == "setbench":
        parse_f = parse_setbench
    else:
        assert(False)

    file_dir = f"{input_dir}/{suite}"

    if experiment != "geomean":
        return parse_f(f"{file_dir}/{experiment}")
    else:
        experiment_set = ["sizes", "updates", "threads"]
        #also add trackers experiments for setbench
        if suite == "setbench":
            experiment_set.append("trackers")
        data = []
        crashes = []
        for exp in experiment_set:
            d, c = parse_f(f"{file_dir}/{exp}")
            data.extend(d)
            crashes.extend(c)
        return data, crashes


# -- Plot 1: Throughput vs key_size (100% writes) -----------------------------
def plot_size(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss)

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows).intersection(ALLOCS))
            sizes  = sorted(set(r["key_size"] for r in ds_rows))

            for alloc in allocs:
                pts = {r["key_size"]: r["gmean"] for r in ds_rows if r["allocator"] == alloc}
                ys = [pts.get(s, None) for s in sizes]
                ax.plot(range(len(sizes)),
                        ys,
                        label=alloc,
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"], 
                        linestyle=FIG_CONFIGS["linestyle"],
                        zorder=ALLOC_ZORDER.get(alloc))

            xlabels = get_nice_scinot_labels(sizes)
            plt.xticks(range(len(sizes)), xlabels)
            ax.set_xlabel("Size (n)")
            ax.set_title(f'{DS_LABELS.get(ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{write_dir}size_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/size_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/size.{fmt}")


# -- Plot 2: Throughput vs update rate -----------------------------
def plot_update(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        dss = sorted(set(r["ds"] for r in data))
        paper_ds = which_paper_ds(dss)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows).intersection(ALLOCS))
            updates  = sorted(set(r["update"] for r in ds_rows))

            for alloc in allocs:
                pts = {r["update"]: r["gmean"] for r in ds_rows if r["allocator"] == alloc}
                ys = [pts.get(s, None) for s in updates]
                ax.plot(range(len(updates)),
                        ys,
                        label=alloc,
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"], 
                        linestyle=FIG_CONFIGS["linestyle"],
                        zorder=ALLOC_ZORDER.get(alloc))

            xlabels = updates
            plt.xticks(range(len(updates)), xlabels)
            ax.set_xlabel("Update (%)")
            ax.set_title(f'{DS_LABELS.get(ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{write_dir}update_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/update_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/update.{fmt}")


# -- Plot 3: Throughput vs update rate -----------------------------
def plot_threads(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        dss = sorted(set(r["ds"] for r in data))
        paper_ds = which_paper_ds(dss)

        thread_counts = set()
        for r in data:
            thread_counts.add(r['threads'])
        thread_counts = sorted(thread_counts)

        for i, ds in enumerate(dss):
            allocs = sorted(set(r["allocator"] for r in data if r["ds"] == ds).intersection(ALLOCS))

            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r.get("ds") == ds]
            threads  = sorted(set(r["threads"] for r in ds_rows))

            for alloc in allocs:
                pts = {r["threads"]: r["gmean"] for r in ds_rows if r["allocator"] == alloc}
                ys = [pts.get(s, None) for s in threads]
                ax.plot(range(len(threads)),
                        ys,
                        label=alloc,
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"], 
                        linestyle=FIG_CONFIGS["linestyle"],
                        zorder=ALLOC_ZORDER.get(alloc))

            xlabels = threads
            plt.xticks(range(len(threads)), xlabels, rotation=90)
            ax.set_xlabel("Thread count")
            ax.set_title(f'{DS_LABELS.get(ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{write_dir}threads_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/threads_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/threads.{fmt}")




# -- Plot 4: Geomean Bars per data structure -----------------------
def plot_geomean(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    bar_width = 0.10
    inter_group_gap = 1.0
    intra_group_gap = 0.02

    dss = sorted(set(r["ds"] for r in data))
    
    szx, szy = FIG_CONFIGS["figsize"]
    fig, ax = plt.subplots(figsize=(len(dss), szy))
    
    seen_allocs = set()
    all_values_global = {}

    for i, ds in enumerate(dss):
        ds_rows = [r for r in data if r["ds"] == ds]
        allocs = sorted(set(r["allocator"] for r in ds_rows).intersection(ALLOCS))
        nbars = len(allocs)
    
        width = 0.8 / max(nbars, 1)
        
        group_width = nbars * bar_width + (nbars - 1) * intra_group_gap
        group_start = i * (group_width + inter_group_gap * bar_width)
        x = np.arange(len(allocs))
    
        bars = []
        per_struct = {}
        for j, alloc in enumerate(allocs):
            ds_rows_alloc = [r for r in ds_rows if r["allocator"] == alloc]
            all_values = []
            for r in ds_rows_alloc:
                all_values.extend(r["values"])

                if alloc not in all_values_global:
                    all_values_global[alloc] = []
                all_values_global[alloc].extend(r["values"])
            y = stat.geometric_mean(all_values)
            per_struct[alloc] = y
    
        best_performing = max([per_struct[alloc] for alloc in allocs])
    
        for j, alloc in enumerate(allocs):
            label = alloc if alloc not in seen_allocs else None
            seen_allocs.add(alloc)

            offset = group_start + j * (bar_width + intra_group_gap)
            y = per_struct[alloc] / best_performing
            bars.append((
                    ax.bar(offset,
                    y,
                    width=bar_width,
                    hatch=ALLOC_HATCHES.get(alloc),
                    color=ALLOC_PALETTE.get(alloc),
                    edgecolor="black",
                    linewidth=FIG_CONFIGS.get("bar_linewidth"),
                    label=label,
                    zorder=ALLOC_ZORDER.get(alloc)),
                    per_struct[alloc]
            ))
    
    
        for bar, ys in bars:
            for b in bar:
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    b.get_height()*1.015+0.01,
                    f'{ys:.1f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=4.5,
                    rotation=90,
                    zorder=ALLOC_ZORDER.get("deqalloc")+1,
                )

        group_center = group_start + (group_width - intra_group_gap) / 2
        ax.text(
            group_center,
            -0.05,  # just below x-axis, in axes coordinates
            DS_LABELS.get(ds, ds),
            ha='center',
            va='top',
            fontsize=FIG_CONFIGS.get("xtick_fontsize")-3,
            transform=ax.get_xaxis_transform(),  # x in data coords, y in axes coords
        )

    #for alloc in all_values_global.keys():
    #    gm = stat.geometric_mean(all_values_global[alloc])
    #    sd = stat.stdev(all_values_global[alloc])
    #    print(alloc, gm, (sd/gm)*100)
    
    #claude.ai aligned bars!
    last_group_start = (len(dss) - 1) * (group_width + inter_group_gap * bar_width)
    first_bar_center = 0  # group_start when i=0, j=0
    last_bar_center = last_group_start + (nbars - 1) * (bar_width + intra_group_gap)
    margin = bar_width / 2 + bar_width * inter_group_gap
    ax.set_xlim(first_bar_center - margin, last_bar_center + margin)

    ax.set_ylim(0, 1.25)
    ax.set_yticks(np.arange(0, 1.1, 0.2))

    plt.xticks([])
    ax.set_xlabel(f"Data Structure ({suite})", labelpad=13)
    
    #ax.legend(
    #    ncol=len(allocs),
    #    frameon=True,
    #    fontsize=FIG_CONFIGS.get("legend_fontsize"),
    #    loc="upper center",
    #    alignment="center",
    #    bbox_to_anchor=(0.5, 1.155),
    #    labelcolor="black",
    #    edgecolor="black",
    #    fancybox=False,
    #    handlelength=2,
    #    handleheight=1,
    #    handletextpad=0.5,
    #    columnspacing=2.17,
    #)
    #ax.get_legend().get_frame().set_linewidth(0.8)

    if suite == SUITES[0]:
        ax.set_ylabel("Geomean Throughput (Mops/s)")
        current_x, current_y = ax.yaxis.label.get_position()
        ax.yaxis.set_label_coords(current_x-0.075, 0.36)
    else:
        ax.set_yticks([])

    style_fig(fig, ax, True)

    #override some style_fig
    ax.grid(visible=False)
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-3.7)
    ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"]-4.5)
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"]-3.5)

    os.makedirs(f"{out_dir}/paper/", exist_ok=True)
    fig.savefig(f"{out_dir}/paper/geomean.{fmt}",
        dpi=FIG_CONFIGS["dpi"],
        bbox_inches="tight",
        pad_inches=FIG_CONFIGS["pad_inches"])
    plt.close(fig)


# -- Plot 5: Throughput in various reclamation schemes -----------------------------
def plot_trackers(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    bar_width = 0.05
    inter_group_gap = 2.0
    intra_group_gap = 0.01

    dss = sorted(set(r["ds"] for r in data))
    trackers = sorted(set(r["reclamation"] for r in data))

    szx, szy = FIG_CONFIGS["figsize"]
    fig, ax = plt.subplots(figsize=(len(trackers)*1.15, szy))

    seen_allocs = set()
    all_values_global = {}

    for i, tracker in enumerate(trackers):
        tracker_rows = [r for r in data if r["reclamation"] == tracker]
        allocs = sorted(set(r["allocator"] for r in tracker_rows).intersection(ALLOCS))

        nbars = len(allocs)
        width = 0.8 / max(nbars, 1)
        
        group_width = nbars * bar_width + (nbars - 1) * intra_group_gap
        group_start = i * (group_width + inter_group_gap * bar_width)
        x = np.arange(len(allocs))
    
        bars = []
        per_struct = {}

        #reduce to alloc, gmean pairs where each gmean represents all ds
        for alloc in allocs:
            ds_rows_alloc = [r for r in tracker_rows if r["allocator"] == alloc]
            all_values = []
            for r in ds_rows_alloc:
                all_values.extend(r["values"])
                if alloc not in all_values_global:
                    all_values_global[alloc] = []
                all_values_global[alloc].extend(r["values"])
            y = stat.geometric_mean(all_values)
            per_struct[alloc] = y
    
        best_performing = max([per_struct[alloc] for alloc in allocs])

        for j, alloc in enumerate(allocs):
            label = alloc if alloc not in seen_allocs else None
            seen_allocs.add(alloc)

            offset = group_start + j * (bar_width + intra_group_gap)
            y = per_struct[alloc] / best_performing

            bars.append((
                    ax.bar(offset,
                    y,
                    width=bar_width,
                    hatch=ALLOC_HATCHES.get(alloc),
                    color=ALLOC_PALETTE.get(alloc),
                    edgecolor="black",
                    linewidth=FIG_CONFIGS.get("bar_linewidth"),
                    label=label,
                    zorder=ALLOC_ZORDER.get(alloc)),
                    per_struct[alloc]
            ))
    
        for bar, ys in bars:
            for b in bar:
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    b.get_height()*1.015+0.01,
                    f'{ys:.1f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=6,
                    rotation=90,
                    zorder=ALLOC_ZORDER.get("deqalloc")+1,
                )

        group_center = group_start + (group_width - intra_group_gap) / 2
        ax.text(
            group_center,
            -0.05,  # just below x-axis, in axes coordinates
            tracker,
            ha='center',
            va='top',
            fontsize=FIG_CONFIGS.get("xtick_fontsize")-1,
            transform=ax.get_xaxis_transform(),  # x in data coords, y in axes coords
        )
    
    #claude.ai aligned bars!
    last_group_start = (len(trackers) - 1) * (group_width + inter_group_gap * bar_width)
    first_bar_center = 0  # group_start when i=0, j=0
    last_bar_center = last_group_start + (nbars - 1) * (bar_width + intra_group_gap)
    margin = bar_width / 2 + bar_width * inter_group_gap
    ax.set_xlim(first_bar_center - margin, last_bar_center + margin)

    ax.set_ylim(0, 1.26)
    ax.set_yticks(np.arange(0, 1.1, 0.2))

    plt.xticks([])
    ax.set_xlabel("Reclamation Scheme", labelpad=15)
    
    ax.legend(
        ncol=len(allocs),
        frameon=True,
        fontsize=FIG_CONFIGS.get("legend_fontsize")+1,
        loc="upper center",
        alignment="center",
        bbox_to_anchor=(0.5, 1.24),
        labelcolor="black",
        edgecolor="black",
        fancybox=False,
        handlelength=2,
        handleheight=1,
        handletextpad=0.5,
        columnspacing=0.63,
    )
    ax.get_legend().get_frame().set_linewidth(0.8)

    ax.set_ylabel("Geomean Throughput (Mops/s)")
    
    style_fig(fig, ax, True)

    #override some style_fig
    ax.grid(visible=False)
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-1.5)
    ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"]-1.5)
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"]-1)

    fig.savefig(f"{out_dir}/paper/trackers.{fmt}",
        dpi=FIG_CONFIGS["dpi"],
        bbox_inches="tight",
        pad_inches=FIG_CONFIGS["pad_inches"])
    plt.close(fig)

# -- Plot 6: Memory usage -----------------------------
def plot_memory(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss)

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            allocs = sorted(set(r["allocator"] for r in data).intersection(ALLOCS))
            sizes  = sorted(set(r["key_size"] for r in data if r["ds"] == ds))

            for alloc in allocs:
                throughput = {r["key_size"]: r["gmean"] for r in data if r["allocator"] == alloc}
                memusage = {r["key_size"]: r["mem_kb"] for r in data if r["allocator"] == alloc}
                ys = [memusage.get(s, 0) / (10**6) for s in sizes] #convert to gb

                ax.plot(range(len(sizes)),
                        ys,
                        label=alloc,
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"], 
                        linestyle=FIG_CONFIGS["linestyle"],
                        zorder=ALLOC_ZORDER.get(alloc))

            xlabels = get_nice_scinot_labels(sizes)
            plt.xticks(range(len(sizes)), xlabels)
            ax.set_xlabel("Size (n)")
            ax.set_title(f'{DS_LABELS.get(ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Memory Usage (GB)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{write_dir}memory_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/memory_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/memory.{fmt}")


def plot_hugepages(input_dir, suite, experiment, out_dir, fmt):
    #load hugepages file (hugepages = never)
    nohp_data, nohp_crashes = load_file(input_dir, suite, experiment)
    #also load sizes (hugepages = always)
    hp_data, nohp_crashes = load_file(input_dir, suite, "sizes")

    dss = sorted(set(r["ds"] for r in nohp_data))
    assert(dss == sorted(set(r["ds"] for r in hp_data)))

    paper_ds = which_paper_ds(dss)

    for paper_print in [True, False]: #print a paper version and a viewing version
        for relative in [True, False]:
            write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
            os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

            for i, ds in enumerate(dss):
                fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])
                fig_comp, ax_comp = plt.subplots(figsize=FIG_CONFIGS["figsize"])

                nohp_ds_rows = [r for r in nohp_data if r["ds"] == ds]
                hp_ds_dows = [r for r in hp_data if r["ds"] == ds]

                allocs = sorted(set(r["allocator"] for r in nohp_ds_rows).intersection(ALLOCS))
                sizes  = sorted(set(r["key_size"] for r in nohp_ds_rows))
                x_positions = range(len(sizes))

                for alloc in allocs:
                    pts_nohp = {r["key_size"]: r["gmean"] for r in nohp_ds_rows if r["allocator"] == alloc}
                    ys_nohp = [pts_nohp.get(s, None) for s in sizes]
                    
                    pts_hp = {r["key_size"]: r["gmean"] for r in hp_ds_dows if r["allocator"] == alloc}
                    ys_hp = [pts_hp.get(s, None) for s in sizes]

                    if not relative:
                        ax.plot(x_positions,
                                ys_nohp,
                                label=alloc,
                                linewidth=FIG_CONFIGS["linewidth"],
                                color=ALLOC_PALETTE.get(alloc),
                                marker=ALLOC_MARKERS.get(alloc),
                                markersize=FIG_CONFIGS["markersize"], 
                                linestyle=FIG_CONFIGS["linestyle"],
                                zorder=ALLOC_ZORDER.get(alloc))

                        ax.plot(x_positions,
                                ys_hp,
                                label=alloc,
                                linewidth=FIG_CONFIGS["linewidth"],
                                color=ALLOC_PALETTE.get(alloc),
                                marker=ALLOC_MARKERS.get(alloc),
                                markersize=FIG_CONFIGS["markersize"], 
                                linestyle="solid",
                                zorder=ALLOC_ZORDER.get(alloc))
                    else:
                        relative_ys = []
                        for a, b in zip(ys_nohp, ys_hp):
                            if b != 0:
                                relative_ys.append(a / b)
                            else:
                                relative_ys.append(0)

                        ax.plot(x_positions,
                                relative_ys,
                                label=alloc,
                                linewidth=FIG_CONFIGS["linewidth"],
                                color=ALLOC_PALETTE.get(alloc),
                                marker=ALLOC_MARKERS.get(alloc),
                                markersize=FIG_CONFIGS["markersize"], 
                                linestyle=FIG_CONFIGS["linestyle"],
                                zorder=ALLOC_ZORDER.get(alloc))

                xlabels = get_nice_scinot_labels(sizes)
                ax.set_xticks(x_positions)
                ax.set_xticklabels(xlabels)
                ax.set_xlabel("Size (n)")
                ax.set_title(f'{DS_LABELS.get(ds)}')

                if not write_dir or ds == paper_ds[0]:
                    ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                    ylabel = ax.yaxis.label
                    ylabel.set_y(ylabel.get_position()[1] - 0.05)

                style_fig(fig, ax, paper_print)
                fig.savefig(f"{out_dir}/{write_dir}hugepages{'_relative' if relative else ''}_{ds}.{fmt}",
                    dpi=FIG_CONFIGS["dpi"],
                    bbox_inches="tight",
                    pad_inches=FIG_CONFIGS["pad_inches"])
                plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/hugepages.{fmt}")

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_relative_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/hugepages_relative.{fmt}")


def plot_temp_and_freq(file, out_dir, fmt):
    if not os.path.exists(file):
        return
    df = pd.read_csv(file)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Temp_C'] = df['Temp_mC'] / 1000.0           # milliCelsius to Celsius
    df['Freq_GHz'] = df['Freq_kHz'] / 1_000_000.0   # kHz to GHz
    fig, ax1 = plt.subplots(figsize=(12, 6))
    color_temp = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Temperature (°C)', color=color_temp, fontsize=12)
    ax1.plot(df['Timestamp'], df['Temp_C'], color=color_temp, marker='o', linestyle='-', linewidth=2, label='Temperature')
    ax1.tick_params(axis='y', labelcolor=color_temp)
    ax1.grid(True, linestyle='--', alpha=0.6)
    ax2 = ax1.twinx()
    color_freq = 'tab:blue'
    ax2.set_ylabel('Frequency (GHz)', color=color_freq, fontsize=12)
    ax2.plot(df['Timestamp'], df['Freq_GHz'], color=color_freq, marker='x', linestyle='--', linewidth=2, label='Frequency')
    ax2.tick_params(axis='y', labelcolor=color_freq)
    style_fig(fig, ax1)
    style_fig(fig, ax2)
    fig.autofmt_xdate()
    plt.title('Device Temperature and Frequency over Time', fontsize=14, pad=15)
    fig.tight_layout()
    fig.savefig(f"{out_dir}/temperature.{fmt}")


# -- Main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Plot deqalloc experiments')
    parser.add_argument('-i', '--input_dir', type=str,
                       help='Path to directory containing timing files')
    parser.add_argument('-ih', '--hugepage_input_dir', type=str, default=None,
                       help='Path to directory containing timing files for hugepage experiments')
    parser.add_argument('-o', '--output-dir', type=str, default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('-b', '--benchmark', type=str, default='all', choices=['flock', 'setbench', 'all'],
                       help='Benchmark suite to plot (default: all)')
    parser.add_argument('--plots', nargs='+',
                       choices=['size',
                                'update',
                                'geomean',
                                'threads',
                                'trackers',
                                'memory',
                                'hugepages',
                                #'ablation',
                                #'machines',
                                'all'],
                       default=['all'],
                       help='Which plots to generate (default: all)')
    parser.add_argument('--format', type=str, choices=['pdf', 'png', 'svg'],
                       default='pdf', help='Output format (default: pdf)')
    #parser.add_argument('--machine-dirs', nargs='+', metavar='LABEL:DIR',
    #                   help='Machine data dirs for multi-machine plot (e.g. Intel:/path/to/dir AMD:/path/to/dir)')

    args = parser.parse_args()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    do_all = "all" in args.plots

    if args.benchmark == "all" or args.benchmark == "flock":
        out_dir = f"{args.output_dir}/flock"
        if "size"        in args.plots or do_all:    plot_size(args.input_dir, "flock", "sizes", out_dir, args.format)
        if "update"      in args.plots or do_all:  plot_update(args.input_dir, "flock", "updates", out_dir, args.format)
        if "threads"     in args.plots or do_all: plot_threads(args.input_dir, "flock", "threads", out_dir, args.format)
        if "memory"      in args.plots or do_all:  plot_memory(args.input_dir, "flock", "sizes", out_dir, args.format)
        if "geomean"     in args.plots or do_all: plot_geomean(args.input_dir, "flock", "geomean", out_dir, args.format)
        if "hugepages" in args.plots or do_all: plot_hugepages(args.input_dir, "flock", "hugepages", out_dir, args.format)

    if args.benchmark == "all" or args.benchmark == "setbench":
        out_dir = f"{args.output_dir}/setbench"
        if "size"        in args.plots    or do_all: plot_size(args.input_dir, "setbench", "sizes", out_dir, args.format)
        if "update"      in args.plots  or do_all: plot_update(args.input_dir, "setbench", "updates", out_dir, args.format)
        if "threads"     in args.plots or do_all: plot_threads(args.input_dir, "setbench", "threads", out_dir, args.format)
        if "memory"      in args.plots  or do_all: plot_memory(args.input_dir, "setbench", "sizes", out_dir, args.format)
        if "trackers"   in args.plots or do_all: plot_trackers(args.input_dir, "setbench", "trackers", out_dir, args.format)
        if "geomean"     in args.plots or do_all: plot_geomean(args.input_dir, "setbench", "sizes", out_dir, args.format)
        if "hugepages" in args.plots or do_all: plot_hugepages(args.input_dir, "setbench", "hugepages", out_dir, args.format)

    plot_temp_and_freq(f"{args.input_dir}/temperature.csv", args.output_dir, args.format)

    if args.benchmark == "all":
        paper_ds_list = [ f"{args.output_dir}/{s}/paper/geomean.{args.format}" for s in SUITES ] 
        merge_pdfs_horizontally(paper_ds_list, f"{args.output_dir}/joined_geomean.{args.format}")

if __name__ == "__main__":
    main()
