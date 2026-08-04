#!/usr/bin/env python3

import warnings
warnings.filterwarnings("ignore", message=".*Unable to import Axes3D.*")

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
from matplotlib.lines import Line2D
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

#set from --verbose in main(); gates the "Reasonable difference in gmean"
#diagnostic prints in parse_flock/parse_setbench
VERBOSE = False

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
    "deqalloc_remotefree": "#ef5350",
    "deqalloc_genericdeque": "#cda434",
}

ALLOCS = ["deqalloc", "mimalloc", "jemalloc", "snmalloc", "hoard", "tcmalloc", "tbbmalloc", "lockfree", "rpmalloc"]

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

DEFAULT_PARAMS = {
    "update": 100,
    "size": {
        "normal": 200000,
        "list": 500,
    },
    "reclamation": "debra",
    #for memory experiments larger sizes matter more
    "memory": {
        "size": {
            "normal": 20000000,
            "list": 10000,
        }
    }
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
    'deqalloc_remotefree': 'X',
    'deqalloc_genericdeque': '>',
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

ALLOC_RENAMES = {
    'deqalloc_remotefree': 'deqalloc-rf',
    'deqalloc_genericdeque': 'deqalloc-gd',
    'deqalloc_localseglist': 'deqalloc-lsl',
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


linestyle_tuple = {
     'loosely dotted'        :  (0, (1, 10)),
     'dotted'                :  (0, (1, 5)),
     'densely dotted'        :  (0, (1, 1)),

     'long dash with offset' :  (5, (10, 3)),
     'loosely dashed'        :  (0, (5, 10)),
     'dashed'                :  (0, (5, 5)),
     'densely dashed'        :  (0, (5, 1)),

     'loosely dashdotted'    :  (0, (3, 10, 1, 10)),
     'dashdotted'            :  (0, (3, 5, 1, 5)),
     'densely dashdotted'    :  (0, (3, 1, 1, 1)),

     'dashdotdotted'         :  (0, (3, 5, 1, 5, 1, 5)),
     'loosely dashdotdotted' :  (0, (3, 10, 1, 10, 1, 10)),
     'densely dashdotdotted' :  (0, (3, 1, 1, 1, 1, 1))
}

FIG_CONFIGS = {
    "figsize": (2.5, 1.5),
    "linewidth": 1.4,
    "markersize": 4,
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
    "linestyle": {
        "deqalloc":  linestyle_tuple["densely dashed"],
        "mimalloc":  linestyle_tuple["dashed"],
        "jemalloc":  linestyle_tuple["long dash with offset"],
        "snmalloc":  linestyle_tuple["densely dashdotdotted"],
        "hoard":     linestyle_tuple["densely dashdotted"],
        "tcmalloc":  linestyle_tuple["dashdotted"],
        "tbbmalloc": linestyle_tuple["densely dotted"],
        "lockfree":  linestyle_tuple["loosely dashed"],
        "rpmalloc":  linestyle_tuple["loosely dashdotted"],
        "deqalloc_remotefree": linestyle_tuple["solid"] if "solid" in linestyle_tuple else (0, ()),
        "deqalloc_genericdeque": linestyle_tuple["densely dashdotted"],
    },
}


#which data structures/trackers to show for the paper for the varying plots
PAPER_DS_FLOCK = ["skiplist_lck", "leaftree_lck", "hash_block_lck"]
PAPER_DS_LOCALSEGLIST_FLOCK = ["skiplist_lck", "leaftree_lck", "list_lck"]
#PAPER_DS_REMOTEBATCHSIZE_FLOCK = ["btree_lck", "hash_block_lck", "leaftree_lck"]
PAPER_DS_REMOTEBATCHSIZE_FLOCK = ["skiplist_lck", "btree_lck", "hash_block_lck"]

#PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hm_hashtable", "hmlist"]
PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hmlist"]
PAPER_TRACKERS_SETBENCH = ["ibr", "debra", "he", "hp", "ebr", "nbr+", "qsbr", "wfe"]
PAPER_DS_LOCALSEGLIST_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hmlist"]
PAPER_DS_REMOTEBATCHSIZE_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hmlist"]

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

    #ax.tick_params(axis='y', labelrotation=90)

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
    try:
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
                    ds = m.group(3)
                    #crashes often, ignore
                    if ds == "arttree_lck": continue
                    entry = dict(
                        allocator=m.group(1),
                        update=int(m.group(2)),
                        ds=ds,
                        key_size=int(m.group(4)),
                        threads=int(m.group(5)),
                        numa=m.group(6) == "True",
                        thread_flags=m.group(7).strip(),
                        values=vals,
                        mean=mean,
                        gmean=gmean,
                        mem_kb=float(m.group(10)),
                        reclamation="debra", #hacky way to integrate with other suites
                        df=False,
                    )
                    rows.append(entry)
                    if VERBOSE and mean != 0 and gmean != 0 and \
                        abs(mean - gmean) / max(gmean, mean) > 0.05:
                        print("Reasonable difference in gmean", entry)
    except FileNotFoundError:
        None
    return rows, crashes

def parse_setbench(path):
    rows = []
    crashes = []
    crash_re = re.compile(r"#\s*CRASH:\s*(\w+)\s+alloc=(\w+)\s+u=(\d+)\s+n=(\d+)")
    row_re   = re.compile(
        r"^(\w+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(\d+)\s+(True|False)\s+\[([^\]]*)\]\s+([\d.]+),\s*([\d.]+)\s*KB"
    )
    try:
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
                        df='_df' in m.group(3),
                    )
                    rows.append(entry)
                    if VERBOSE and mean != 0 and gmean != 0 and \
                        abs(mean - gmean) / max(gmean, mean) > 0.05:
                        print("Reasonable difference in gmean", entry)
    except FileNotFoundError:
        None
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

def merge_pdfs_vertically(pdf_list, output_path):
    if pdf_list == []:
        return
    if not pdfmerge: #package not imported
        return
    docs = [fitz.open(pdf) for pdf in pdf_list]
    pages = [doc[0] for doc in docs]
    total_height = sum(page.rect.height for page in pages)
    max_width = max(page.rect.width for page in pages)
    out_doc = fitz.open()
    out_page = out_doc.new_page(width=max_width, height=total_height)
    current_y = 0
    for i, page in enumerate(pages):
        x_offset = (max_width - page.rect.width) / 2
        rect = fitz.Rect(x_offset, current_y, x_offset + page.rect.width, current_y + page.rect.height)
        out_page.show_pdf_page(rect, docs[i], 0)
        current_y += page.rect.height
    out_doc.save(output_path)
    for doc in docs:
        doc.close()
    out_doc.close()
    print(f"merged {len(pdf_list)} pdfs vertically to {output_path}")

def which_paper_ds(dss, experiment=None):
    paper_ds = []
    if set(dss).intersection(set(PAPER_DS_FLOCK)):
        if not experiment:
            paper_ds = PAPER_DS_FLOCK
        elif experiment.startswith("ablation_localseglist") \
            or experiment == "ablation_remotefree":
            paper_ds = PAPER_DS_LOCALSEGLIST_FLOCK
        elif experiment == "ablation_remotefree_batchsize":
            paper_ds = PAPER_DS_REMOTEBATCHSIZE_FLOCK
    if set(dss).intersection(set(PAPER_DS_SETBENCH)):
        assert(paper_ds == [])
        if not experiment:
            paper_ds = PAPER_DS_SETBENCH
        elif experiment.startswith("ablation_localseglist") \
            or experiment == "amortizedfree":
            paper_ds = PAPER_DS_LOCALSEGLIST_SETBENCH
        elif experiment == "ablation_remotefree_batchsize":
            paper_ds = PAPER_DS_REMOTEBATCHSIZE_SETBENCH
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
        #if suite == "setbench":
        #    experiment_set.append("trackers")
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
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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
    all_ds_list = [ f"{out_dir}/paper/{experiment}/size_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/size_all.{fmt}")


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
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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
    all_ds_list = [ f"{out_dir}/paper/{experiment}/update_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/update_all.{fmt}")


# -- Plot 2b: Throughput vs zipfian skew (-z parameter) -----------------------
def parse_zipfian_skew(thread_flags):
    """Extract the zipfian skew value from a thread_flags string like '-z 0.6'."""
    m = re.search(r"-z\s*([\d.]+)", thread_flags or "")
    if m:
        return float(m.group(1))
    return None

def plot_zipfian(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)
    if not data:
        return

    #zipfian skew comes from the thread_flags field (e.g. "-z 0.6"), not a
    #dedicated column, so parse it out here rather than in parse_flock
    for r in data:
        r["zipf"] = parse_zipfian_skew(r.get("thread_flags", ""))
    data = [r for r in data if r["zipf"] is not None]
    if not data:
        return

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        dss = sorted(set(r["ds"] for r in data))
        paper_ds = which_paper_ds(dss)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows).intersection(ALLOCS))
            zipfs  = sorted(set(r["zipf"] for r in ds_rows))

            for alloc in allocs:
                pts = {r["zipf"]: r["gmean"] for r in ds_rows if r["allocator"] == alloc}
                ys = [pts.get(s, None) for s in zipfs]
                ax.plot(range(len(zipfs)),
                        ys,
                        label=alloc,
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"],
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc),
                        zorder=ALLOC_ZORDER.get(alloc))

            xlabels = [str(z) for z in zipfs]
            plt.xticks(range(len(zipfs)), xlabels)
            ax.set_xlabel("Zipfian skew")
            ax.set_title(f'{DS_LABELS.get(ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{write_dir}zipfian_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/zipfian_{ds}.{fmt}" for ds in paper_ds ]
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/zipfian.{fmt}")
    all_ds_list = [ f"{out_dir}/paper/{experiment}/zipfian_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/zipfian_all.{fmt}")


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
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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
    all_ds_list = [ f"{out_dir}/paper/{experiment}/threads_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/threads_all.{fmt}")




# -- Plot 4: Geomean Bars per data structure -----------------------
def plot_geomean(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    bar_width = 0.10
    inter_group_gap = 1.0
    intra_group_gap = 0.02

    dss = sorted(set(r["ds"] for r in data))
    #if "list_lck" in dss:
    #    dss.remove("list_lck")
    #if "skiplist_lck" in dss:
    #    dss.remove("skiplist_lck")

    szx, szy = FIG_CONFIGS["figsize"]
    fig, ax = plt.subplots(figsize=(len(dss) + 1, szy*0.8))

    seen_allocs = set()
    all_values_global = {}

    # -- first pass: compute per-ds (allocator -> gmean throughput) without
    # drawing anything yet. This lets us build the "average" group (computed
    # separately, by pooling raw values across every ds) before any bars are
    # placed on the axes, so it can be rendered as the leftmost group. --
    ds_per_struct = {}
    for ds in dss:
        ds_rows = [r for r in data if r["ds"] == ds]
        allocs = sorted(set(r["allocator"] for r in ds_rows).intersection(ALLOCS))
        per_struct = {}
        for alloc in allocs:
            ds_rows_alloc = [r for r in ds_rows if r["allocator"] == alloc]
            all_values = []
            for r in ds_rows_alloc:
                all_values.extend(r["values"])
                all_values_global.setdefault(alloc, []).extend(r["values"])
            per_struct[alloc] = stat.geometric_mean(all_values) if len(all_values) > 0 else 0
        ds_per_struct[ds] = (allocs, per_struct)

    # -- "average" group: compiled separately (its own pooled raw values,
    # not an average of the already-normalized per-ds bars), and using the
    # arithmetic mean (per the "mean" part of each row) rather than the
    # geomean used for the individual data-structure groups. --
    avg_allocs = sorted(all_values_global.keys())
    avg_per_struct = {
        alloc: (stat.mean(all_values_global[alloc]) if all_values_global[alloc] else 0)
        for alloc in avg_allocs
    }

    groups = [("average", avg_allocs, avg_per_struct)]
    groups += [(ds, ds_per_struct[ds][0], ds_per_struct[ds][1]) for ds in dss]

    last_group_start = 0
    last_group_width = 0
    last_nbars = 0

    for i, (label, allocs, per_struct) in enumerate(groups):
        nbars = len(allocs)
        group_width = nbars * bar_width + (nbars - 1) * intra_group_gap
        group_start = i * (group_width + inter_group_gap * bar_width)

        best_performing = max([per_struct[alloc] for alloc in allocs]) if allocs else 0

        bars = []
        for j, alloc in enumerate(allocs):
            bar_label = alloc if alloc not in seen_allocs else None
            seen_allocs.add(alloc)

            offset = group_start + j * (bar_width + intra_group_gap)
            y = per_struct[alloc] #/ best_performing if best_performing else 0
            bars.append((
                    ax.bar(offset,
                    y,
                    width=bar_width,
                    hatch=ALLOC_HATCHES.get(alloc),
                    color=ALLOC_PALETTE.get(alloc),
                    edgecolor="black",
                    linewidth=FIG_CONFIGS.get("bar_linewidth"),
                    label=bar_label,
                    zorder=ALLOC_ZORDER.get(alloc)),
                    per_struct[alloc]
            ))


        for bar, ys in bars:
            for b in bar:
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    b.get_height()*1.015+2,
                    f'{ys:.1f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=4.5,
                    rotation=90,
                    zorder=ALLOC_ZORDER.get("deqalloc")+1,
                )

        group_center = group_start + (group_width - intra_group_gap) / 2
        is_average = (label == "average")
        display_label = "average" if is_average else DS_LABELS.get(label, label)
        ax.text(
            group_center,
            -0.05,  # just below x-axis, in axes coordinates
            display_label,
            ha='center',
            va='top',
            fontsize=FIG_CONFIGS.get("xtick_fontsize")-3,
            fontweight=('bold' if is_average else 'normal'),
            transform=ax.get_xaxis_transform(),  # x in data coords, y in axes coords
        )

        last_group_start = group_start
        last_group_width = group_width
        last_nbars = nbars

    #for alloc in all_values_global.keys():
    #    gm = stat.geometric_mean(all_values_global[alloc])
    #    sd = stat.stdev(all_values_global[alloc])
    #    print(alloc, gm, (sd/gm)*100)

    #claude.ai aligned bars!
    first_bar_center = 0  # group_start when i=0, j=0 (now the "average" group)
    last_bar_center = last_group_start + (last_nbars - 1) * (bar_width + intra_group_gap)
    margin = bar_width / 2 + bar_width * inter_group_gap
    ax.set_xlim(first_bar_center - margin, last_bar_center + margin)

    #ax.set_ylim(0, 1.4)
    ax.set_ylim(0, ax.dataLim.ymax * 1.37)
    #ax.set_yticks(np.arange(0, 1.1, 0.2))

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
        ax.set_ylabel("Geomean Throughput\n(Mops/s)")
        current_x, current_y = ax.yaxis.label.get_position()
        ax.yaxis.set_label_coords(current_x-0.075, 0.36)
    else:
        ax.set_yticks([])

    style_fig(fig, ax, True)

    #override some style_fig
    ax.grid(visible=False)
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-2.7)
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
    fig, ax = plt.subplots(figsize=((len(trackers) + 1)*1.15, szy*0.9))

    seen_allocs = set()
    all_values_global = {}

    # -- first pass: compute per-tracker (allocator -> gmean throughput)
    # without drawing, so the "average" group (pooled across every tracker,
    # computed separately) can be built and placed as the leftmost group. --
    tracker_per_struct = {}
    for tracker in trackers:
        tracker_rows = [r for r in data if r["reclamation"] == tracker]
        allocs = sorted(set(r["allocator"] for r in tracker_rows).intersection(ALLOCS))
        per_struct = {}
        for alloc in allocs:
            ds_rows_alloc = [r for r in tracker_rows if r["allocator"] == alloc]
            all_values = []
            for r in ds_rows_alloc:
                all_values.extend(r["values"])
                all_values_global.setdefault(alloc, []).extend(r["values"])
            per_struct[alloc] = stat.geometric_mean(all_values) if all_values else 0
        tracker_per_struct[tracker] = (allocs, per_struct)

    # -- "average" group: compiled separately from pooled raw values across
    # all trackers, using the arithmetic mean rather than the geomean used
    # for the individual tracker groups. --
    avg_allocs = sorted(all_values_global.keys())
    avg_per_struct = {
        alloc: (stat.mean(all_values_global[alloc]) if all_values_global[alloc] else 0)
        for alloc in avg_allocs
    }

    groups = [("average", avg_allocs, avg_per_struct)]
    groups += [(tracker, tracker_per_struct[tracker][0], tracker_per_struct[tracker][1]) for tracker in trackers]

    last_group_start = 0
    last_group_width = 0
    last_nbars = 0

    for i, (label, allocs, per_struct) in enumerate(groups):
        nbars = len(allocs)
        group_width = nbars * bar_width + (nbars - 1) * intra_group_gap
        group_start = i * (group_width + inter_group_gap * bar_width)

        bars = []
        for j, alloc in enumerate(allocs):
            bar_label = alloc if alloc not in seen_allocs else None
            seen_allocs.add(alloc)

            offset = group_start + j * (bar_width + intra_group_gap)
            y = per_struct[alloc]

            bars.append((
                    ax.bar(offset,
                    y,
                    width=bar_width,
                    hatch=ALLOC_HATCHES.get(alloc),
                    color=ALLOC_PALETTE.get(alloc),
                    edgecolor="black",
                    linewidth=FIG_CONFIGS.get("bar_linewidth"),
                    label=bar_label,
                    zorder=ALLOC_ZORDER.get(alloc)),
                    per_struct[alloc]
            ))

        for bar, ys in bars:
            for b in bar:
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    b.get_height()*1.015+0.015,
                    f'{ys:.1f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=6,
                    rotation=90,
                    zorder=ALLOC_ZORDER.get("deqalloc")+1,
                )

        group_center = group_start + (group_width - intra_group_gap) / 2
        is_average = (label == "average")
        ax.text(
            group_center,
            -0.05,  # just below x-axis, in axes coordinates
            label,
            ha='center',
            va='top',
            fontsize=FIG_CONFIGS.get("xtick_fontsize")-1,
            fontweight=('bold' if is_average else 'normal'),
            transform=ax.get_xaxis_transform(),  # x in data coords, y in axes coords
        )

        last_group_start = group_start
        last_group_width = group_width
        last_nbars = nbars

    #claude.ai aligned bars!
    first_bar_center = 0  # group_start when i=0, j=0 (now the "average" group)
    last_bar_center = last_group_start + (last_nbars - 1) * (bar_width + intra_group_gap)
    margin = bar_width / 2 + bar_width * inter_group_gap
    ax.set_xlim(first_bar_center - margin, last_bar_center + margin)

    #ax.set_ylim(0, 1.26)
    #ax.set_yticks(np.arange(0, 1.1, 0.2))
    ax.set_ylim(0, ax.dataLim.ymax * 1.37)


    plt.xticks([])
    ax.set_xlabel("Reclamation Scheme", labelpad=15)

    ax.legend(
        ncol=len(avg_allocs),
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

    ax.set_ylabel("Geomean Throughput\n(Mops/s)")
    
    style_fig(fig, ax, True)

    #override some style_fig
    ax.grid(visible=False)
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-2.7)
    ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"]-2.5)
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
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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
    all_ds_list = [ f"{out_dir}/paper/{experiment}/memory_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/memory_all.{fmt}")


def plot_hugepages(input_dir, suite, experiment, out_dir, fmt):
    #load hugepages file (hugepages = never)
    nohp_data, nohp_crashes = load_file(input_dir, suite, experiment)
    #also load sizes (hugepages = always)
    hp_data, nohp_crashes = load_file(input_dir, suite, "sizes")

    if nohp_data == [] or hp_data == []:
        return

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

                min_relative_y = 20000

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
                                linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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
                            if b != 0 and b != None:
                                rel = a / b
                                relative_ys.append(rel)
                                min_relative_y=min(min_relative_y, rel)
                            else:
                                relative_ys.append(0)



                        ax.plot(x_positions,
                                relative_ys,
                                label=alloc,
                                linewidth=FIG_CONFIGS["linewidth"],
                                color=ALLOC_PALETTE.get(alloc),
                                marker=ALLOC_MARKERS.get(alloc),
                                markersize=FIG_CONFIGS["markersize"], 
                                linestyle=FIG_CONFIGS["linestyle"].get(alloc),
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

                if relative:
                    ax.set_ylim(bottom=min_relative_y * 0.98)

                fig.savefig(f"{out_dir}/{write_dir}hugepages{'_relative' if relative else ''}_{ds}.{fmt}",
                    dpi=FIG_CONFIGS["dpi"],
                    bbox_inches="tight",
                    pad_inches=FIG_CONFIGS["pad_inches"])
                plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/hugepages.{fmt}")

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_relative_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/hugepages_relative.{fmt}")

    all_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/hugepages_all.{fmt}")

    all_ds_list = [ f"{out_dir}/paper/{experiment}/hugepages_relative_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/hugepages_relative_all.{fmt}")

# -- plot ablation experiments
def plot_ablation_localseglist(input_dir, suite, experiment, out_dir, fmt):
    """
    Ablation of shared vs. thread-local Segment lists (deqalloc vs
    deqalloc-lsl). One panel per data structure, and for each panel two
    figures are produced:

      - throughput: ratio deqalloc / variant, so >1 means deqalloc (shared
                    Segments) is faster.
      - memory:     ratio variant / deqalloc, so >1 means deqalloc (shared
                    Segments) used LESS memory.

    Both metrics are oriented so that "above the 1.0 reference line means
    sharing Segments wins", which makes the two figures directly comparable
    instead of having one inverted relative to the other.

    The x-axis follows the experiment file: "*_threads" sweeps the thread
    count, anything else sweeps key_size.
    """
    data, crashes = load_file(input_dir, suite, experiment)
    if not data:
        print(f"WARNING: no data for experiment={experiment} suite={suite}; skipping")
        return

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss, experiment)

    #the ablation is run both as a size sweep and as a thread sweep; pick the
    #x-axis off the filename rather than always assuming key_size
    if experiment.endswith("_threads"):
        xfield, xaxis_label, xrotation = "threads", "Thread count", 90
    else:
        xfield, xaxis_label, xrotation = "key_size", "Size (n)", 0

    # metric_key: row field on the y-axis.
    #   "gmean"  -> throughput, ratio deqalloc/variant
    #   "mem_kb" -> peak RSS,   ratio variant/deqalloc (inverted: less is better)
    def _run(metric_key, filename_infix, ylabel_text, invert):
        rendered_dss = set()

        #shared y-axis floor across every ds so that panels merged side by
        #side use the same scale, rather than each panel picking its own
        global_min = 1.0
        for ds in dss:
            ds_rows_g = [r for r in data if r["ds"] == ds]
            xs_g = sorted(set(r[xfield] for r in ds_rows_g))
            for alloc in sorted(set(r["allocator"] for r in ds_rows_g)):
                if alloc == "deqalloc":
                    continue
                for x in xs_g:
                    a = _agg(ds_rows_g, "deqalloc", xfield, x, metric_key)
                    b = _agg(ds_rows_g, alloc, xfield, x, metric_key)
                    rat = _ratio(a, b, invert)
                    if rat is not None:
                        global_min = min(global_min, rat)
        if math.isnan(global_min) or math.isinf(global_min):
            global_min = 0

        for paper_print in [True, False]: #paper version and viewing version
            write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
            os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

            for i, ds in enumerate(dss):
                fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

                ds_rows = [r for r in data if r["ds"] == ds]
                allocs = sorted(set(r["allocator"] for r in ds_rows))
                xs = sorted(set(r[xfield] for r in ds_rows))

                plotted_any = False
                for alloc in allocs:
                    #deqalloc is the baseline, it is the 1.0 line
                    if alloc == "deqalloc": continue

                    relative_ys = []
                    for x in xs:
                        a = _agg(ds_rows, "deqalloc", xfield, x, metric_key)
                        b = _agg(ds_rows, alloc, xfield, x, metric_key)
                        relative_ys.append(_ratio(a, b, invert))

                    if not any(y is not None for y in relative_ys):
                        continue
                    plotted_any = True

                    ax.plot(range(len(xs)),
                            relative_ys,
                            label=ALLOC_RENAMES.get(alloc, alloc),
                            linewidth=FIG_CONFIGS["linewidth"],
                            color=ALLOC_PALETTE.get(alloc),
                            marker=ALLOC_MARKERS.get(alloc),
                            markersize=FIG_CONFIGS["markersize"],
                            linestyle=FIG_CONFIGS["linestyle"].get(alloc, "--"),
                            zorder=ALLOC_ZORDER.get(alloc))

                if not plotted_any:
                    print(f"WARNING: no comparable rows for ds={ds} "
                          f"(experiment={experiment}, metric={filename_infix}); skipping panel")
                    plt.close(fig)
                    continue

                #1.0 means the two variants performed identically
                ax.axhline(1.0, color="black", linewidth=0.8, linestyle=":", zorder=0)

                if xfield == "key_size":
                    xlabels = get_nice_scinot_labels(xs)
                else:
                    xlabels = [str(x) for x in xs]
                plt.xticks(range(len(xs)), xlabels, rotation=xrotation)
                ax.set_xlabel(xaxis_label)
                ax.set_title(f'{DS_LABELS.get(ds, ds)}')

                if not paper_ds or ds == paper_ds[0]:
                    ax.set_ylabel(ylabel_text, fontsize=FIG_CONFIGS["ylabel_fontsize"])
                    ylabel = ax.yaxis.label
                    ylabel.set_y(ylabel.get_position()[1] - 0.05)

                style_fig(fig, ax, paper_print)

                #override style_fig's bottom=0: these are ratios living around
                #1, so anchor on the smallest ratio seen across all panels
                ax.set_ylim(bottom=global_min * 0.97)

                fig.savefig(f"{out_dir}/{write_dir}{experiment}{filename_infix}_{ds}.{fmt}",
                    dpi=FIG_CONFIGS["dpi"],
                    bbox_inches="tight",
                    pad_inches=FIG_CONFIGS["pad_inches"])
                plt.close(fig)
                rendered_dss.add(ds)

        paper_list = [ f"{out_dir}/paper/{experiment}/{experiment}{filename_infix}_{ds}.{fmt}"
                       for ds in paper_ds if ds in rendered_dss ]
        merge_pdfs_horizontally(paper_list,
            f"{out_dir}/paper/{experiment}{filename_infix}.{fmt}")

        all_list = [ f"{out_dir}/paper/{experiment}/{experiment}{filename_infix}_{ds}.{fmt}"
                     for ds in dss if ds in rendered_dss ]
        merge_pdfs_horizontally(all_list,
            f"{out_dir}/paper/{experiment}{filename_infix}_all.{fmt}")

    #throughput: deqalloc/variant, >1 means sharing Segments is faster
    _run("gmean",  "",        "Relative Throughput", invert=False)
    #memory: variant/deqalloc, >1 means sharing Segments uses less memory
    _run("mem_kb", "_memory", "Relative Memory",     invert=True)


def _agg(rows, alloc, xfield, x, metric_key):
    """Mean of metric_key over every row for (alloc, x). Returns None if the
    configuration was not run, 0 if it ran but produced nothing (a crash)."""
    vals = [r[metric_key] for r in rows
            if r["allocator"] == alloc and r[xfield] == x]
    if not vals:
        return None
    return stat.mean(vals)


def _ratio(deq, other, invert):
    """deq/other, or other/deq when invert (i.e. for lower-is-better metrics
    such as memory), so that >1 always means deqalloc won."""
    if deq is None or other is None:
        return None
    num, den = (other, deq) if invert else (deq, other)
    if den in (None, 0):
        return None
    return num / den


# -- Plot: Throughput vs key_size, normal vs amortized-free reclamation ------
def plot_ablation_amortizedfree(input_dir, suite, experiment, out_dir, fmt):
    """
    Same layout as the other ablation plots (one panel per DS, x-axis is
    key_size). For each DS this produces two figures:
      - absolute:  every allocator gets two lines, a solid line for the
                    normal reclamation scheme and a dashed line (same
                    color/marker) for its amortized-free ("_df") counterpart.
      - relative:  one line per allocator, plotting the ratio
                    (amortized-free throughput / normal throughput), with a
                    dotted reference line at 1.0 so speedups/regressions are
                    visible directly.
    """
    data, crashes = load_file(input_dir, suite, experiment)

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss, experiment)

    allocators = [ "deqalloc", "mimalloc", "jemalloc", "hoard", "snmalloc" ] 

    #precompute a single shared y-axis floor for the *relative* panel, used
    #for every ds, so that panels merged side by side (e.g. bst-tk next to
    #abtree/hmlist) share the same scale instead of each getting its own
    #independent bottom limit (which made some panels look "lower" than
    #others once merged, purely because their own min ratio happened to be
    #closer to/farther from 1.0)
    global_min_relative = 1
    for ds in dss:
        ds_rows_g = [r for r in data if r["ds"] == ds]
        allocs_g  = sorted(set(r["allocator"] for r in ds_rows_g).intersection(allocators))
        sizes_g   = sorted(set(r["key_size"] for r in ds_rows_g))
        for alloc in allocs_g:
            base_pts_g = {r["key_size"]: r["gmean"] for r in ds_rows_g
                          if r["allocator"] == alloc and not r["df"]}
            df_pts_g   = {r["key_size"]: r["gmean"] for r in ds_rows_g
                          if r["allocator"] == alloc and r["df"]}
            for s in sizes_g:
                b = base_pts_g.get(s)
                d = df_pts_g.get(s)
                if d is not None and b not in (None, 0):
                    global_min_relative = min(global_min_relative, d / b)
    if math.isnan(global_min_relative) or math.isinf(global_min_relative):
        global_min_relative = 0

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        for i, ds in enumerate(dss):
            ds_rows = [r for r in data if r["ds"] == ds]
            allocs  = sorted(set(r["allocator"] for r in ds_rows).intersection(allocators))
            sizes   = sorted(set(r["key_size"] for r in ds_rows))
            xlabels = get_nice_scinot_labels(sizes)

            #pre-compute normal/amortized-free points per allocator, shared
            #by both the absolute and the relative panel below
            per_alloc = {}
            for alloc in allocs:
                base_pts = {r["key_size"]: r["gmean"] for r in ds_rows
                            if r["allocator"] == alloc and not r["df"]}
                df_pts   = {r["key_size"]: r["gmean"] for r in ds_rows
                            if r["allocator"] == alloc and r["df"]}
                per_alloc[alloc] = (base_pts, df_pts)

            # -- absolute throughput panel ---------------------------------
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            for alloc in allocs:
                color  = ALLOC_PALETTE.get(alloc)
                marker = ALLOC_MARKERS.get(alloc)
                zorder = ALLOC_ZORDER.get(alloc)
                base_pts, df_pts = per_alloc[alloc]

                base_ys = [base_pts.get(s, None) for s in sizes]
                df_ys   = [df_pts.get(s, None) for s in sizes]

                if any(y is not None for y in base_ys):
                    ax.plot(range(len(sizes)),
                            base_ys,
                            label=ALLOC_RENAMES.get(alloc, alloc),
                            linewidth=FIG_CONFIGS["linewidth"],
                            color=color,
                            marker=marker,
                            markersize=FIG_CONFIGS["markersize"],
                            linestyle="-",
                            zorder=zorder)

                if any(y is not None for y in df_ys):
                    ax.plot(range(len(sizes)),
                            df_ys,
                            label=None, #same allocator, avoid duplicate legend entries
                            linewidth=FIG_CONFIGS["linewidth"],
                            color=color,
                            marker=marker,
                            markersize=FIG_CONFIGS["markersize"],
                            markerfacecolor="none",
                            linestyle="--",
                            zorder=zorder)

            plt.xticks(range(len(sizes)), xlabels)
            ax.set_xlabel("Size (n)")
            ax.set_title(f'{DS_LABELS.get(ds, ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)

            fig.savefig(f"{out_dir}/{write_dir}{experiment}_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

            # -- relative throughput panel (amortized-free / normal) --------
            fig_r, ax_r = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            for alloc in allocs:
                color  = ALLOC_PALETTE.get(alloc)
                marker = ALLOC_MARKERS.get(alloc)
                zorder = ALLOC_ZORDER.get(alloc)
                base_pts, df_pts = per_alloc[alloc]

                relative_ys = []
                for s in sizes:
                    b = base_pts.get(s)
                    d = df_pts.get(s)
                    relative_ys.append(d / b if (d is not None and b not in (None, 0)) else None)

                if any(y is not None for y in relative_ys):
                    ax_r.plot(range(len(sizes)),
                            relative_ys,
                            label=ALLOC_RENAMES.get(alloc, alloc),
                            linewidth=FIG_CONFIGS["linewidth"],
                            color=color,
                            marker=marker,
                            markersize=FIG_CONFIGS["markersize"],
                            linestyle=FIG_CONFIGS["linestyle"].get(alloc, "--"),
                            zorder=zorder)

            #reference line: 1.0 means amortized-free performs the same as normal
            ax_r.axhline(1.0, color="black", linewidth=0.8, linestyle=":", zorder=0)

            plt.xticks(range(len(sizes)), xlabels)
            ax_r.set_xlabel("Size (n)")
            ax_r.set_title(f'{DS_LABELS.get(ds, ds)}')

            if not write_dir or ds == paper_ds[0]:
                ax_r.set_ylabel('Relative Throughput',
                                 fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel_r = ax_r.yaxis.label
                ylabel_r.set_y(ylabel_r.get_position()[1] - 0.05)

            style_fig(fig_r, ax_r, paper_print)

            #override style_fig's bottom=0 default: ratios legitimately live
            #around 1, so anchor the bottom near the lowest ratio observed
            #across ALL data structures (not just this one), so that every
            #panel shares the same y-axis scale once merged side by side
            ax_r.set_ylim(bottom=global_min_relative * 0.95)

            fig_r.savefig(f"{out_dir}/{write_dir}{experiment}_relative_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig_r)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/{experiment}_{ds}.{fmt}" for ds in paper_ds ]
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/{experiment}.{fmt}")

    paper_ds_list_relative = [ f"{out_dir}/paper/{experiment}/{experiment}_relative_{ds}.{fmt}" for ds in paper_ds ]
    merge_pdfs_horizontally(paper_ds_list_relative, f"{out_dir}/paper/{experiment}_relative.{fmt}")

    all_ds_list = [ f"{out_dir}/paper/{experiment}/{experiment}_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/{experiment}_all.{fmt}")

    all_ds_list_relative = [ f"{out_dir}/paper/{experiment}/{experiment}_relative_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list_relative, f"{out_dir}/paper/{experiment}_relative_all.{fmt}")


def plot_ablation_remotefree(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss, experiment)

    for paper_print in [True, False]: #print a paper version and a viewing version
        write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
        os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows))
            sizes  = sorted(set(r["key_size"] for r in ds_rows))

            for alloc in allocs:
                #dont plot deqalloc
                pts = {r["key_size"]: r["gmean"] for r in ds_rows if r["allocator"] == alloc}
                ys = [pts.get(s, None) for s in sizes]

                #benchmark crashed, skip it
                #if 0 in deqalloc_ys: continue
                #relative_ys = [a / b if b != 0 else 0 for a, b in zip(deqalloc_ys, ys)]

                ax.plot(range(len(sizes)),
                        ys,
                        label=ALLOC_RENAMES.get(alloc, alloc),
                        linewidth=FIG_CONFIGS["linewidth"],
                        color=ALLOC_PALETTE.get(alloc),
                        marker=ALLOC_MARKERS.get(alloc),
                        markersize=FIG_CONFIGS["markersize"], 
                        linestyle=FIG_CONFIGS["linestyle"].get(alloc, "--"),
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

            #override style_fig
            min_y = min(ax.dataLim.ymin, 1)
            if math.isnan(min_y) or math.isinf(min_y):
                min_y = 0
            ax.set_ylim(bottom=min_y * 0.99)

            ax.legend(
                ncol=len(allocs),
                frameon=True,
                fontsize=FIG_CONFIGS.get("legend_fontsize"),
                loc="upper center",
                alignment="center",
                bbox_to_anchor=(0.5, 1.40),
                labelcolor="black",
                edgecolor="black",
                fancybox=False,
            )

            fig.savefig(f"{out_dir}/{write_dir}{experiment}_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/{experiment}/{experiment}_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/{experiment}.{fmt}")
    all_ds_list = [ f"{out_dir}/paper/{experiment}/{experiment}_{ds}.{fmt}" for ds in dss ]
    merge_pdfs_horizontally(all_ds_list, f"{out_dir}/paper/{experiment}_all.{fmt}")


# -- Plot X: Throughput vs Remote Free Batch Size -----------------------------
def plot_remotefree_batchsize(input_dir, suite, experiment, out_dir, fmt):
    data, crashes = load_file(input_dir, suite, experiment)

    dss = sorted(set(r["ds"] for r in data))
    paper_ds = which_paper_ds(dss, experiment)

    # Helper to parse allocator name and batch size from your specific format
    def parse_allocator(name):
        if name == "deqalloc": return "deqalloc", 16384
        if name == "deqalloc_remotefree": return "deqalloc_remotefree", 16384
        if name == "deqalloc_genericdeque": return "deqalloc_genericdeque", 16384
        
        m = re.match(r'^(.*)_(\d+)$', name)
        if m:
            return m.group(1), int(m.group(2))
        return name, 16384 # fallback

    #base allocators appearing anywhere in this experiment's data, used to
    #build a single legend for the aggregate (merged) throughput plot
    base_allocs_global = sorted(set(parse_allocator(r["allocator"])[0] for r in data))

    # metric_key: which field in the row we plot on the y-axis.
    # "gmean" -> throughput (Mops/s); "mem_kb" -> memory usage (GB, converted below).
    def _run(metric_key, filename_prefix, ylabel_text, convert_gb=False):
        rendered_dss = set()
        for paper_print in [True, False]: # print a paper version and a viewing version
            write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
            os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

            for i, ds in enumerate(dss):
                fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

                default_size = DEFAULT_PARAMS["memory"]["size"].get(DS_TYPES.get(ds))
                ds_rows = [r for r in data if r["ds"] == ds and r["key_size"] == default_size]

                if not ds_rows:
                    print(f"WARNING: no rows for ds={ds} at key_size={default_size} "
                          f"(experiment={experiment}, suite={suite}, metric={filename_prefix}); skipping panel")
                    plt.close(fig)
                    continue

                parsed_rows = []
                for r in ds_rows:
                    base_alloc, b_size = parse_allocator(r["allocator"])
                    r_new = dict(r)
                    r_new["base_allocator"] = base_alloc
                    r_new["batch_size"] = b_size
                    parsed_rows.append(r_new)

                base_allocs = sorted(set(r["base_allocator"] for r in parsed_rows))

                # Find the max number of variants to size the X-axis properly
                max_variants = max(len(set(r["batch_size"] for r in parsed_rows if r["base_allocator"] == alloc)) for alloc in base_allocs)

                # Grab deqalloc's sizes specifically to use as the baseline X-axis labels
                deq_sizes = sorted(set(r["batch_size"] for r in parsed_rows if r["base_allocator"] == "deqalloc"))
                if not deq_sizes:
                    deq_sizes = list(range(max_variants)) # Fallback if deqalloc is missing

                for alloc in base_allocs:
                    # Sort the batch sizes for THIS specific allocator
                    alloc_sizes = sorted(set(r["batch_size"] for r in parsed_rows if r["base_allocator"] == alloc))

                    if metric_key == "mem_kb":
                        pts = {}
                        for s in alloc_sizes:
                            vals = [r["mem_kb"] for r in parsed_rows
                                    if r["base_allocator"] == alloc and r["batch_size"] == s]
                            pts[s] = stat.mean(vals) if vals else None
                    else:
                        pts = {r["batch_size"]: r["gmean"] for r in parsed_rows if r["base_allocator"] == alloc}

                    # ys are ordered from smallest variant to largest variant
                    ys = [pts.get(s, None) for s in alloc_sizes]
                    if convert_gb:
                        ys = [ (y / (10**6)) if y is not None else None for y in ys ]

                    color = ALLOC_PALETTE.get(alloc, "#9e9e9e")
                    marker = ALLOC_MARKERS.get(alloc, "o")
                    linestyle = FIG_CONFIGS["linestyle"].get(alloc, "-") if "linestyle" in FIG_CONFIGS else "-"
                    zorder = ALLOC_ZORDER.get(alloc, 0)

                    # Plot against rank (0, 1, 2...) instead of absolute byte size
                    ax.plot(range(len(alloc_sizes)),
                            ys,
                            label=ALLOC_RENAMES.get(alloc, alloc),
                            linewidth=FIG_CONFIGS["linewidth"],
                            color=color,
                            marker=marker,
                            markersize=FIG_CONFIGS["markersize"],
                            linestyle=linestyle,
                            zorder=zorder)

                # Generate X-axis labels using deqalloc's byte scale
                xlabels = [fmt_size(b) if isinstance(b, int) else str(b) for b in deq_sizes]

                # Pad labels in case another allocator has more variants than deqalloc
                while len(xlabels) < max_variants:
                    xlabels.append("")

                plt.xticks(range(max_variants))

                # Adjust the x-axis label to clarify what the scale represents
                ax.set_xlabel("Batch Size Rank")
                ax.set_title(f'{DS_LABELS.get(ds, ds)}')

                if not write_dir or ds == paper_ds[0]:
                    ax.set_ylabel(ylabel_text, fontsize=FIG_CONFIGS["ylabel_fontsize"])
                    ylabel = ax.yaxis.label
                    ylabel.set_y(ylabel.get_position()[1] - 0.05)

                style_fig(fig, ax, paper_print)

                max_y = ax.dataLim.ymax
                if max_y > 0:
                    ax.set_ylim(top=max_y * 1.05)

                fig.savefig(f"{out_dir}/{write_dir}{filename_prefix}_{ds}.{fmt}",
                    dpi=FIG_CONFIGS["dpi"],
                    bbox_inches="tight",
                    pad_inches=FIG_CONFIGS["pad_inches"])
                plt.close(fig)
                rendered_dss.add(ds)

            paper_ds_list = [ f"{out_dir}/paper/{experiment}/{filename_prefix}_{ds}.{fmt}" for ds in paper_ds if ds in rendered_dss]
            paper_merged_path = f"{out_dir}/paper/{experiment}_{filename_prefix}.{fmt}" if filename_prefix != "batchsize" else f"{out_dir}/paper/{experiment}.{fmt}"
            if paper_ds_list:
                merge_pdfs_horizontally(paper_ds_list, paper_merged_path)

            all_ds_list = [ f"{out_dir}/paper/{experiment}/{filename_prefix}_{ds}.{fmt}" for ds in dss if ds in rendered_dss ]
            all_merged_path = f"{out_dir}/paper/{experiment}_{filename_prefix}_all.{fmt}" if filename_prefix != "batchsize" else f"{out_dir}/paper/{experiment}_all.{fmt}"
            if all_ds_list:
                merge_pdfs_horizontally(all_ds_list, all_merged_path)

            #single legend on top of the aggregate plot (not on every individual
            #ds panel), flock throughput only
            if suite == "flock" and filename_prefix == "batchsize" and base_allocs_global:
                legend_path = f"{out_dir}/paper/{experiment}/{filename_prefix}_legend.{fmt}"
                generate_legend(base_allocs_global, legend_path.rsplit(f".{fmt}", 1)[0], fmt)

                if paper_ds_list:
                    tmp_path = paper_merged_path + ".tmp"
                    merge_pdfs_vertically([legend_path, paper_merged_path], tmp_path)
                    os.replace(tmp_path, paper_merged_path)
                if all_ds_list:
                    tmp_path = all_merged_path + ".tmp"
                    merge_pdfs_vertically([legend_path, all_merged_path], tmp_path)
                    os.replace(tmp_path, all_merged_path)

    # throughput (existing behaviour, kept as "batchsize_*")
    _run("gmean", "batchsize", "Throughput (Mops/s)", convert_gb=False)
    # memory usage (new): "batchsize_memory_*", merged into "{experiment}_memory[.{fmt}|_all.{fmt}]"
    _run("mem_kb", "batchsize_memory", "Memory Usage (GB)", convert_gb=True)


#Function was mostly AI generated (Claude)
def plot_config(input_dir, suite, experiment, out_dir, fmt):
    """
    Grouped bar chart: for each data structure, bars are grouped by base
    allocator. Within each group, one bar per (numa, df) variant so the
    viewer can immediately see which combination wins for each allocator.
    A geomean-across-all-DS chart is prepended as the leftmost panel.

    Styled to match the other bar charts in this file (plot_geomean /
    plot_trackers): same bar geometry constants, same hatch/edge treatment,
    and the same style_fig() override block (grid off, matching font sizes).
    """
    data, crashes = load_file(input_dir, suite, experiment)
    has_df   = any(r["df"] for r in data)
    variants = ["base", "numa", "df", "numa+df"] if has_df else ["base", "numa"]
    nv       = len(variants)

    #use the same palette family as the rest of the plots: reuse ALLOC_PALETTE
    #hues is inappropriate here (variants aren't allocators), so keep a small
    #dedicated palette but align hatches with the ALLOC_HATCHES vocabulary
    #used everywhere else for visual consistency.
    VARIANT_COLORS = {
        "base":    "#4fc3f7",
        "numa":    "#81c784",
        "df":      "#ffb74d",
        "numa+df": "#ef5350",
    }
    VARIANT_HATCHES = {
        "base":    "",
        "numa":    "---",
        "df":      "///",
        "numa+df": "xxx",
    }

    #match plot_geomean's bar geometry
    bar_w     = 0.10
    intra_gap = 0.02
    inter_gap = 1.0
    group_w   = nv * bar_w + (nv - 1) * intra_gap

    paper_print = True
    write_dir = ("paper/" if paper_print else "readable/") + experiment + "/"
    os.makedirs(f"{out_dir}/{write_dir}", exist_ok=True)

    def _variant_key(r):
        if r["numa"] and r["df"]: return "numa+df"
        if r["numa"]:             return "numa"
        if r["df"]:               return "df"
        return "base"

    def _draw_bars(ax, allocs, rows):
        """Pool raw values by (allocator, variant), draw grouped bars, return tick_xs."""
        pool = defaultdict(list)
        for r in rows:
            vkey = _variant_key(r)
            if r["allocator"] in allocs and vkey in variants:
                pool[(r["allocator"], vkey)].extend(r["values"])

        seen, tick_xs = set(), []
        for i, alloc in enumerate(allocs):
            group_start = i * (group_w + inter_gap * bar_w)
            for j, vkey in enumerate(variants):
                vals = pool.get((alloc, vkey), [])
                y    = stat.geometric_mean(vals) if vals else 0.0
                x    = group_start + j * (bar_w + intra_gap)
                ax.bar(x, y,
                       width=bar_w,
                       color=VARIANT_COLORS[vkey],
                       hatch=VARIANT_HATCHES[vkey],
                       edgecolor="black",
                       linewidth=FIG_CONFIGS["bar_linewidth"],
                       label=vkey if vkey not in seen else None,
                       zorder=ALLOC_ZORDER.get(alloc, 0))
                seen.add(vkey)
                if y > 0:
                    ax.text(x + bar_w / 2, y * 1.015 + 0.01, f"{y:.2f}",
                            ha="center", va="bottom",
                            fontweight='bold',
                            fontsize=4.5, rotation=90,
                            zorder=ALLOC_ZORDER.get("deqalloc", 0) + 1)
            tick_xs.append(group_start + (group_w - intra_gap) / 2)
        return tick_xs

    def _save(fig, ax, allocs, tick_xs, title, path, show_legend):
        #labels sit below the axis, like plot_geomean's per-group labels,
        #instead of matplotlib's default tick labels
        plt.sca(ax)
        plt.xticks([])
        for x, alloc in zip(tick_xs, allocs):
            ax.text(
                x, -0.05,
                ALLOC_RENAMES.get(alloc, alloc),
                ha='center', va='top',
                fontsize=FIG_CONFIGS.get("xtick_fontsize") - 3,
                transform=ax.get_xaxis_transform(),
            )

        margin = bar_w / 2 + bar_w * inter_gap
        ax.set_xlim(tick_xs[0] - group_w / 2 - margin, tick_xs[-1] + group_w / 2 + margin)

        ax.set_ylim(bottom=0, top=ax.dataLim.ymax * 1.3)
        ax.set_ylabel("Throughput (Mops/s)")
        ax.set_xlabel("Allocator", labelpad=13)
        ax.set_title(title)

        if show_legend:
            ax.legend(
                ncol=nv,
                frameon=True,
                fontsize=FIG_CONFIGS.get("legend_fontsize"),
                loc="upper right",
                alignment="center",
                bbox_to_anchor=(1, 1),
                labelcolor="black",
                edgecolor="black",
                fancybox=False,
                handlelength=2,
                handleheight=1,
                handletextpad=0.5,
                columnspacing=0.63,
            )
            ax.get_legend().get_frame().set_linewidth(0.8)

        style_fig(fig, ax, paper_print)

        #override some style_fig, matching plot_geomean / plot_trackers exactly
        ax.grid(visible=False)
        ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"] - 2.7)
        ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"] - 4.5)
        ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"] - 3.5)

        fig.savefig(path, dpi=FIG_CONFIGS["dpi"], bbox_inches="tight",
                    pad_inches=FIG_CONFIGS["pad_inches"])
        plt.close(fig)

    _, szy     = FIG_CONFIGS["figsize"]
    all_allocs = [a for a in ALLOCS if a in {r["allocator"] for r in data}]
    dss        = sorted(set(r["ds"] for r in data))

    # --- Geomean across all DS (leftmost panel) ---
    fig_gm, ax_gm = plt.subplots(figsize=(max(len(all_allocs) * 1.0, 2.5), szy * 0.95))
    tick_xs_gm    = _draw_bars(ax_gm, all_allocs, data)
    gm_path       = f"{out_dir}/{write_dir}config_geomean.{fmt}"
    _save(fig_gm, ax_gm, all_allocs, tick_xs_gm, "Geomean", gm_path, show_legend=True)

    # --- Per-DS panels (already covers all data structures, not just PAPER_DS_X) ---
    per_ds_paths = []
    for ds in dss:
        ds_rows   = [r for r in data if r["ds"] == ds]
        ds_allocs = [a for a in ALLOCS if a in {r["allocator"] for r in ds_rows}]
        fig, ax   = plt.subplots(figsize=(max(len(ds_allocs) * 1.0, 3.5), szy * 1.15))
        tick_xs   = _draw_bars(ax, ds_allocs, ds_rows)
        path      = f"{out_dir}/{write_dir}config_{ds}.{fmt}"
        _save(fig, ax, ds_allocs, tick_xs, DS_LABELS.get(ds, ds), path, show_legend=False)
        per_ds_paths.append(path)

    merge_pdfs_horizontally([gm_path] + per_ds_paths, f"{out_dir}/paper/config.{fmt}")


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




def generate_legend(allocs, out_path, fmt):
    """
    Generates a standalone legend where labels sit directly above their lines/markers.
    Bypasses standard matplotlib legend logic for custom coordinate placement.
    """
    # 1. Create a blank figure. 
    # Bumped the multiplier to 1.5 to give the text slightly more breathing room
    fig, ax = plt.subplots(figsize=(len(allocs) * (1 if len(allocs) > 4 else 1.5), 0.5))

    # 2. Keep the bounding box, but hide the internal graph ticks
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    # 3. Calculate evenly spaced X coordinates using 'center of slots' math.
    # If len=2, slots are width 0.5. Centers will be at 0.25 and 0.75.
    # This guarantees perfect centering regardless of the number of items.
    slot_width = 1.0 / len(allocs)
    xs = [(i + 0.5) * slot_width for i in range(len(allocs))]

    # 4. Draw the items directly onto the canvas
    for x, alloc in zip(xs, allocs):
        # Draw the text label in the upper half (y=0.65)
        ax.text(x, 0.65, ALLOC_RENAMES.get(alloc, alloc), ha='center', va='center')

        # Draw the line and marker directly below the text (y=0.35)
        # Dynamically bound the line width so it never exceeds its slot
        line_radius = min(0.04, slot_width * 0.35) 
        
        ax.plot([x - line_radius, x + line_radius], [0.35, 0.35],
                color=ALLOC_PALETTE.get(alloc),
                linewidth=FIG_CONFIGS.get("linewidth"),
                marker=ALLOC_MARKERS.get(alloc),
                markersize=FIG_CONFIGS.get("markersize"),
                linestyle=FIG_CONFIGS.get("linestyle").get(alloc),
                clip_on=False)

    # Save the custom legend
    fig.savefig(f"{out_path}.{fmt}",
                dpi=FIG_CONFIGS.get("dpi", 300),
                bbox_inches="tight",
                pad_inches=0.05) # Keeps a slight margin inside the box
    plt.close(fig)


def collect_variance_stats(input_dir):
    """Scan every raw results file under input_dir/{flock,setbench}/ exactly
    once (independent of which plots were requested, so a run's stddev
    isn't double-counted just because its file backs multiple plots), and
    compute each row's stddev across its repeated throughput samples.

    Returns (by_allocator, by_benchmark): dicts mapping allocator name /
    "suite/filename" to the list of stddevs observed for it.
    """
    by_allocator = defaultdict(list)
    by_benchmark = defaultdict(list)

    for suite, parse_f in (("flock", parse_flock), ("setbench", parse_setbench)):
        suite_dir = os.path.join(input_dir, suite)
        if not os.path.isdir(suite_dir):
            continue
        for fname in sorted(os.listdir(suite_dir)):
            path = os.path.join(suite_dir, fname)
            if not os.path.isfile(path) or fname.startswith('.'):
                continue
            rows, _ = parse_f(path)
            for r in rows:
                samples = r["values"]
                if len(samples) < 2:
                    continue
                mean = stat.mean(samples)
                if mean == 0:
                    continue
                stddev = stat.stdev(samples)
                cv = stddev / mean  # coefficient of variation
                by_allocator[r["allocator"]].append(cv)
                by_benchmark[f"{suite}/{fname}"].append(cv)
    return by_allocator, by_benchmark


def print_variance_stats(input_dir):
    by_allocator, by_benchmark = collect_variance_stats(input_dir)

    if not by_allocator and not by_benchmark:
        return

    print("\n=== Average relative stddev (CV) by allocator (across all benchmarks) ===")
    for alloc in sorted(by_allocator):
        vals = by_allocator[alloc]
        print(f"  {alloc:<24} {stat.mean(vals) * 100:>9.2f}%  (n={len(vals)})")

    print("\n=== Average relative stddev (CV) by benchmark (across all allocators) ===")
    for bench in sorted(by_benchmark):
        vals = by_benchmark[bench]
        print(f"  {bench:<40} {stat.mean(vals) * 100:>9.2f}%  (n={len(vals)})")


# -- Biggest deqalloc improvement over each allocator, across everything -----
def _config_key(r, suite):
    """Config identity (everything except the allocator/values/derived
    fields) used to match up deqalloc against another allocator run under
    otherwise identical settings."""
    if suite == "flock":
        return (r["update"], r["ds"], r["key_size"], r["threads"], r["numa"], r["thread_flags"])
    else:
        return (r["update"], r["ds"], r["key_size"], r["threads"], r["numa"], r["reclamation"], r["df"])


def collect_best_improvements(input_dir):
    """Scan every raw results file (same set collect_variance_stats scans)
    and, for every config where both deqalloc and some other allocator were
    run, compute deqalloc's speedup (gmean ratio) over that allocator. Keep
    the single biggest speedup seen per allocator, with enough context to
    report where it came from.

    Returns: dict allocator -> dict(ratio, suite, file, ds, config_key)
    """
    best = {}

    for suite, parse_f in (("flock", parse_flock), ("setbench", parse_setbench)):
        suite_dir = os.path.join(input_dir, suite)
        if not os.path.isdir(suite_dir):
            continue
        for fname in sorted(os.listdir(suite_dir)):
            path = os.path.join(suite_dir, fname)
            if not os.path.isfile(path) or fname.startswith('.'):
                continue
            rows, _ = parse_f(path)
            if not rows:
                continue

            by_config = defaultdict(dict) #config_key -> {allocator: row}
            for r in rows:
                if r["gmean"] <= 0:
                    continue
                by_config[_config_key(r, suite)][r["allocator"]] = r

            for cfg, alloc_rows in by_config.items():
                deq_row = alloc_rows.get("deqalloc")
                if deq_row is None:
                    continue
                for alloc, r in alloc_rows.items():
                    if alloc == "deqalloc" or r["gmean"] <= 0:
                        continue
                    ratio = deq_row["gmean"] / r["gmean"]
                    prev = best.get(alloc)
                    if prev is None or ratio > prev["ratio"]:
                        best[alloc] = dict(
                            ratio=ratio,
                            suite=suite,
                            file=fname,
                            ds=r["ds"],
                            deq_gmean=deq_row["gmean"],
                            other_gmean=r["gmean"],
                            config=cfg,
                        )
    return best


def print_best_improvements(input_dir):
    best = collect_best_improvements(input_dir)
    if not best:
        return

    print("\n=== Biggest deqalloc speedup over each allocator (across all experiments) ===")
    for alloc in sorted(best, key=lambda a: -best[a]["ratio"]):
        b = best[alloc]
        print(f"  deqalloc vs {alloc:<22} {b['ratio']:>7.2f}x  "
              f"({b['deq_gmean']:.2f} vs {b['other_gmean']:.2f} Mops/s)  "
              f"[{b['suite']}/{b['file']}, ds={b['ds']}]")


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
                                'zipfian',
                                'geomean',
                                'threads',
                                'trackers',
                                'memory',
                                'hugepages',
                                'ablation',
                                'amortizedfree',
                                'config',
                                #'machines',
                                'all'],
                       default=['all'],
                       help='Which plots to generate (default: all)')
    parser.add_argument('--format', type=str, choices=['pdf', 'png', 'svg'],
                       default='pdf', help='Output format (default: pdf)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed diagnostic messages (e.g. gmean/mean discrepancies)')
    #parser.add_argument('--machine-dirs', nargs='+', metavar='LABEL:DIR',
    #                   help='Machine data dirs for multi-machine plot (e.g. Intel:/path/to/dir AMD:/path/to/dir)')

    args = parser.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    do_all = "all" in args.plots

    if args.benchmark == "all" or args.benchmark == "flock":
        out_dir = f"{args.output_dir}/flock"
        if "size"        in args.plots or do_all:    plot_size(args.input_dir, "flock", "sizes", out_dir, args.format)
        if "update"      in args.plots or do_all:  plot_update(args.input_dir, "flock", "updates", out_dir, args.format)
        if "zipfian"     in args.plots or do_all: plot_zipfian(args.input_dir, "flock", "zipfian", out_dir, args.format)
        if "threads"     in args.plots or do_all: plot_threads(args.input_dir, "flock", "threads", out_dir, args.format)
        if "memory"      in args.plots or do_all:  plot_memory(args.input_dir, "flock", "sizes", out_dir, args.format)
        if "geomean"     in args.plots or do_all: plot_geomean(args.input_dir, "flock", "geomean", out_dir, args.format)
        if "hugepages" in args.plots or do_all: plot_hugepages(args.input_dir, "flock", "hugepages", out_dir, args.format)
        if "config" in args.plots or do_all: plot_config(args.input_dir, "flock", "config", out_dir, args.format)
        if "ablation"   in args.plots or do_all:
            for exp in ["ablation_localseglist_sizes", "ablation_localseglist_threads"]:
                plot_ablation_localseglist(args.input_dir, "flock", exp, out_dir, args.format)
        if "ablation" in args.plots or do_all: 
            plot_remotefree_batchsize(args.input_dir, "flock", "ablation_remotefree_batchsize", out_dir, args.format)

    if args.benchmark == "all" or args.benchmark == "setbench":
        out_dir = f"{args.output_dir}/setbench"
        if "size"        in args.plots    or do_all: plot_size(args.input_dir, "setbench", "sizes", out_dir, args.format)
        if "update"      in args.plots  or do_all: plot_update(args.input_dir, "setbench", "updates", out_dir, args.format)
        if "zipfian"     in args.plots or do_all: plot_zipfian(args.input_dir, "setbench", "zipfian", out_dir, args.format)
        if "threads"     in args.plots or do_all: plot_threads(args.input_dir, "setbench", "threads", out_dir, args.format)
        if "memory"      in args.plots  or do_all: plot_memory(args.input_dir, "setbench", "sizes", out_dir, args.format)
        if "trackers"   in args.plots or do_all: plot_trackers(args.input_dir, "setbench", "trackers", out_dir, args.format)
        if "geomean"     in args.plots or do_all: plot_geomean(args.input_dir, "setbench", "geomean", out_dir, args.format)
        if "hugepages" in args.plots or do_all: plot_hugepages(args.input_dir, "setbench", "hugepages", out_dir, args.format)
        if "config" in args.plots or do_all: plot_config(args.input_dir, "setbench", "config", out_dir, args.format)
        if "ablation"   in args.plots or do_all:
            for exp in ["ablation_localseglist_sizes", "ablation_localseglist_threads"]:
                plot_ablation_localseglist(args.input_dir, "setbench", exp, out_dir, args.format)
            #plot_ablation_localseglist(args.input_dir, "setbench", "ablation_localseglist", out_dir, args.format)
        if "amortizedfree" in args.plots or do_all: plot_ablation_amortizedfree(args.input_dir, "setbench", \
            "amortizedfree", out_dir, args.format)
        if "ablation" in args.plots or do_all: 
            plot_remotefree_batchsize(args.input_dir, "setbench", "ablation_remotefree_batchsize", out_dir, args.format)

    plot_temp_and_freq(f"{args.input_dir}/temperature.csv", args.output_dir, args.format)

    if args.benchmark == "all" and do_all or "geomean" in args.plots:
        paper_ds_list = [ f"{args.output_dir}/{s}/paper/geomean.{args.format}" for s in SUITES ] 
        merge_pdfs_horizontally(paper_ds_list, f"{args.output_dir}/joined_geomean.{args.format}")

    generate_legend(ALLOCS, f"{args.output_dir}/legend", args.format)
    generate_legend(["deqalloc", "deqalloc_remotefree", "deqalloc_genericdeque", "snmalloc"], f"{args.output_dir}/legend_batchsize", args.format)

    print_variance_stats(args.input_dir)
    print_best_improvements(args.input_dir)

if __name__ == "__main__":
    main()
