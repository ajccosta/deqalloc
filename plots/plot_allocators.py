#!/usr/bin/env python3

import sys
import re
import math
import os
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
    "deqalloc":  "#4fc3f7",
    "mimalloc":  "#81c784",
    "jemalloc":  "#ffb74d",
    "snmalloc":  "#ce93d8",
    "hoard":     "#f48fb1",
    "tcmalloc":  "#ef5350",
    "tbbmalloc": "#ff7043",
    "lockfree":  "#26c6da",
    "rpmalloc":  "#d4e157",
}

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
    "2geibr_df"    : "ibr",
    "debra_df"     : "debra",
    "he_df"        : "he",
    "ibr_hp_df"    : "hp",
    "ibr_rcu_df"   : "ebr",
    "nbr_df"       : "nbr",
    "nbrplus_df"   : "nbr+",
    "qsbr_df"      : "qsbr",
    "wfe_df"       : "wfe",
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

DEFAULT_PARAMS = {
    "update": 100,
    "size": {
        "normal": 200000,
        "list": 2000,
    },
    "reclamation": "debra",
    "threads": -1,
}

#which data structures/trackers to show for the paper for the varying plots
PAPER_DS_FLOCK = ["skiplist_lck", "leaftree_lck", "hash_block_lck"]

#PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hm_hashtable", "hmlist"]
PAPER_DS_SETBENCH = ["guerraoui_ext_bst_ticket", "brown_ext_abtree_lf", "hm_hashtable"]
PAPER_TRACKERS_SETBENCH = ["ibr", "debra", "he", "hp", "ebr", "nbr+", "qsbr", "wfe"]

mpl.rcParams["hatch.linewidth"] = FIG_CONFIGS.get("bar_linewidth")

def style_fig(fig, ax, paper_print):
    ax.tick_params(axis='x', labelsize=FIG_CONFIGS["xtick_fontsize"])
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"])

    ylabel = ax.yaxis.label
    xlabel = ax.xaxis.label
    xlabel.set_fontsize(FIG_CONFIGS["xlabel_fontsize"])
    ylabel.set_fontsize(FIG_CONFIGS["ylabel_fontsize"])

    ax.title.set_fontsize(fontsize=FIG_CONFIGS["title_fontsize"])
    ax.title.set_fontweight('bold')

    fig.patch.set_edgecolor('none')

    #l, r = ax.get_xlim()
    #margin = FIG_CONFIGS["xtick_end_margin"]
    #ax.set_xlim(l - margin, r + margin)

    ax.set_ylim(bottom=0)

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
        #TODO UPDATE
        r"^(\w+)\s+(\d+)\s+(\w+)\s+(\d+)\s+(True|False)\s+\[([^\]]*)\]\s+([\d.]+),\s*([\d.]+)\s*KB"
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
                vals_str = m.group(6).strip()
                vals = [float(x) for x in vals_str.split()] if vals_str else []
                mean = stat.mean(vals) if len(vals) > 0 else 0
                gmean = stat.geometric_mean(vals) if len(vals) > 0 else 0
                entry = dict(
                    allocator=m.group(1),
                    update=int(m.group(2)),
                    ds=m.group(3),
                    key_size=int(m.group(4)),
                    numa=m.group(5) == "True",
                    values=vals,
                    mean=mean,
                    gmean=gmean,
                    mem_kb=float(m.group(8)),
                )
                rows.append(entry)
                if abs(mean - gmean) > 0.5 * 10**1:
                    print("Reasonable difference in gmean", entry)
                if abs(mean - float(m.group(7))) > 10**-3:
                    print(f"Error in mean checksum. Given: {float(m.group(7))}, Calculated: {mean}")
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


# -- Plot 1: Throughput vs key_size (100% writes) -----------------------------
def plot_size(rows, out_dir, fmt):
    target_update = DEFAULT_PARAMS.get("update")

    data = [r for r in rows if r["update"] == target_update and r["gmean"] > 0]
    dss = sorted(set(r["ds"] for r in data))

    paper_ds = which_paper_ds(dss)

    for paper_print in [True, False]: #print a paper version and a viewing version
        paper_dir = "paper/" if paper_print else ""
        os.makedirs(f"{out_dir}/{paper_dir}", exist_ok=True)

        for i, ds in enumerate(dss):
            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows))
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

            if not paper_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{paper_dir}size_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/size_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/size.{fmt}")


# -- Plot 2: Throughput vs update rate -----------------------------
def plot_update(rows, out_dir, fmt):
    for paper_print in [True, False]: #print a paper version and a viewing version
        paper_dir = "paper/" if paper_print else ""
        os.makedirs(f"{out_dir}/{paper_dir}", exist_ok=True)

        dss = sorted(set(r["ds"] for r in rows))
        paper_ds = which_paper_ds(dss)

        for i, ds in enumerate(dss):

            data = [r for r in rows if r["key_size"] == DEFAULT_PARAMS["size"][DS_TYPES[ds]] and r["gmean"] > 0]

            fig, ax = plt.subplots(figsize=FIG_CONFIGS["figsize"])

            ds_rows = [r for r in data if r["ds"] == ds]
            allocs = sorted(set(r["allocator"] for r in ds_rows))
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

            if not paper_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{paper_dir}update_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/update_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/update.{fmt}")


# -- Plot 3: Throughput vs update rate -----------------------------
def plot_threads(rows, out_dir, fmt):
    for paper_print in [True, False]: #print a paper version and a viewing version
        paper_dir = "paper/" if paper_print else ""
        os.makedirs(f"{out_dir}/{paper_dir}", exist_ok=True)

        dss = sorted(set(r["ds"] for r in rows))
        paper_ds = which_paper_ds(dss)

        thread_counts = set()
        for r in rows:
            thread_counts.add(r['threads'])
        thread_counts = sorted(thread_counts)

        for i, ds in enumerate(dss):
            data = []
            allocs = sorted(set(r["allocator"] for r in rows if r["ds"] == ds))

            for t in thread_counts:
                for a in allocs:
                    d = [r for r in rows if
                        DEFAULT_PARAMS["size"][DS_TYPES[ds]] == r["key_size"] and
                        DEFAULT_PARAMS["reclamation"] in r["reclamation"] and
                        DEFAULT_PARAMS["update"] == r["update"] and
                        r["ds"] == ds and
                        r["allocator"] == a and
                        r["gmean"] > 0 and
                        r["threads"] == t]

                    data.append(merge_entries(d))

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

            if not paper_dir or ds == paper_ds[0]:
                ax.set_ylabel('Throughput (Mops/s)', fontsize=FIG_CONFIGS["ylabel_fontsize"])
                ylabel = ax.yaxis.label
                ylabel.set_y(ylabel.get_position()[1] - 0.05)

            style_fig(fig, ax, paper_print)
            fig.savefig(f"{out_dir}/{paper_dir}threads_{ds}.{fmt}",
                dpi=FIG_CONFIGS["dpi"],
                bbox_inches="tight",
                pad_inches=FIG_CONFIGS["pad_inches"])
            plt.close(fig)

    paper_ds_list = [ f"{out_dir}/paper/threads_{ds}.{fmt}" for ds in paper_ds ] 
    merge_pdfs_horizontally(paper_ds_list, f"{out_dir}/paper/threads.{fmt}")




# -- Plot 4: Geomean Bars per data structure -----------------------
def plot_geomean(rows, out_dir, fmt):
    bar_width = 1
    inter_group_gap = 1.5
    intra_group_gap = 0.3

    dss = sorted(set(r["ds"] for r in rows))
    
    szx, szy = FIG_CONFIGS["figsize"]
    fig, ax = plt.subplots(figsize=(len(ALLOC_PALETTE)*0.6462, szy))
    
    data = [r for r in rows if r["gmean"] > 0]
    seen_allocs = set()

    all_values_global = {}

    for i, ds in enumerate(dss):
        ds_rows = [r for r in data if r["ds"] == ds]
        allocs = sorted(set(r["allocator"] for r in ds_rows))
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
                    fontsize=4,
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
            fontsize=FIG_CONFIGS.get("xtick_fontsize")-1,
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

    plt.xticks([])
    ax.set_xlabel("Data Structure", labelpad=11)
    
    ax.legend(
        ncol=len(allocs),
        frameon=True,
        fontsize=FIG_CONFIGS.get("legend_fontsize"),
        loc="upper center",
        alignment="center",
        bbox_to_anchor=(0.5, 1.155),
        labelcolor="black",
        edgecolor="black",
        fancybox=False,
        handlelength=2,
        handleheight=1,
        handletextpad=0.5,
        columnspacing=2.17,
    )
    ax.get_legend().get_frame().set_linewidth(0.8)

    ax.set_ylabel("Geomean Throughput (Mops/s)")
    
    style_fig(fig, ax, True)

    #override some style_fig
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-1.5)
    ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"]-1.5)
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"]-1)

    fig.savefig(f"{out_dir}/paper/geomean.{fmt}",
        dpi=FIG_CONFIGS["dpi"],
        bbox_inches="tight",
        pad_inches=FIG_CONFIGS["pad_inches"])
    plt.close(fig)


# -- Plot 5: Throughput in various reclamation schemes -----------------------------
def plot_trackers(rows, out_dir, fmt):
    bar_width = 0.6
    inter_group_gap = 1.2
    intra_group_gap = 0.3

    dss = sorted(set(r["ds"] for r in rows))
    trackers = sorted(r["reclamation"] for r in rows)
    trackers = sorted(set(trackers).intersection(set(PAPER_TRACKERS_SETBENCH)))

    szx, szy = FIG_CONFIGS["figsize"]
    fig, ax = plt.subplots(figsize=(len(trackers)*1.15, szy))

    seen_allocs = set()

    all_values_global = {}

    data = [r for r in rows if
        DEFAULT_PARAMS["size"][DS_TYPES[r["ds"]]] == r["key_size"] and
        DEFAULT_PARAMS["threads"] == r["threads"] and
        DEFAULT_PARAMS["update"] == r["update"] and
        #tracker == r["reclamation"].replace("_df", "") and
        r["gmean"] > 0]

    for i, tracker in enumerate(trackers):
        tracker_rows = [r for r in data if r["reclamation"] == tracker]
        allocs = sorted(set(r["allocator"] for r in tracker_rows))

        nbars = len(allocs)
        width = 0.8 / max(nbars, 1)
        
        group_width = nbars * bar_width + (nbars - 1) * intra_group_gap
        group_start = i * (group_width + inter_group_gap * bar_width)
        x = np.arange(len(allocs))
    
        bars = []
        per_struct = {}

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
                    fontsize=4,
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

    #for alloc in all_values_global.keys():
    #    gm = stat.geometric_mean(all_values_global[alloc])
    #    sd = stat.stdev(all_values_global[alloc])
    #    print(alloc, gm, (sd/gm)*100)
    
    #claude.ai aligned bars!
    last_group_start = (len(trackers) - 1) * (group_width + inter_group_gap * bar_width)
    first_bar_center = 0  # group_start when i=0, j=0
    last_bar_center = last_group_start + (nbars - 1) * (bar_width + intra_group_gap)
    margin = bar_width / 2 + bar_width * inter_group_gap
    ax.set_xlim(first_bar_center - margin, last_bar_center + margin)

    ax.set_ylim(0, 1.25)

    plt.xticks([])
    ax.set_xlabel("Reclamation Scheme", labelpad=15)
    
    ax.legend(
        ncol=len(allocs),
        frameon=True,
        fontsize=FIG_CONFIGS.get("legend_fontsize"),
        loc="upper center",
        alignment="center",
        bbox_to_anchor=(0.5, 1.22),
        labelcolor="black",
        edgecolor="black",
        fancybox=False,
        handlelength=2,
        handleheight=1,
        handletextpad=0.5,
        columnspacing=1.65,
    )
    ax.get_legend().get_frame().set_linewidth(0.8)

    ax.set_ylabel("Geomean Throughput (Mops/s)")
    
    style_fig(fig, ax, True)

    #override some style_fig
    ax.yaxis.label.set_fontsize(FIG_CONFIGS["ylabel_fontsize"]-1.5)
    ax.xaxis.label.set_fontsize(FIG_CONFIGS["xlabel_fontsize"]-1.5)
    ax.tick_params(axis='y', labelsize=FIG_CONFIGS["ytick_fontsize"]-1)

    fig.savefig(f"{out_dir}/paper/trackers.{fmt}",
        dpi=FIG_CONFIGS["dpi"],
        bbox_inches="tight",
        pad_inches=FIG_CONFIGS["pad_inches"])
    plt.close(fig)



# -- Main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Plot deqalloc experiments')
    parser.add_argument('-i', '--input_dir', type=str,
                       help='Path to directory containing timing files')
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

    if args.benchmark == "all" or args.benchmark == "flock":
        rows, crashes = parse_flock(f"{args.input_dir}/flock")
        #max_nthreads = max([r["threads"] for r in rows if r["gmean"] > 0])
        do_all = "all" in args.plots
        if "size" in args.plots or do_all: plot_size(rows, f"{args.output_dir}/flock", args.format)
        if "update" in args.plots or do_all: plot_update(rows, f"{args.output_dir}/flock", args.format)
        if "geomean" in args.plots or do_all: plot_geomean(rows, f"{args.output_dir}/flock", args.format)

    if args.benchmark == "all" or args.benchmark == "setbench":
        rows, crashes = parse_setbench(f"{args.input_dir}/setbench")
        nthreads = sorted(set([r["threads"] for r in rows if r["gmean"] > 0]))
        #index -1 is oversubscribed, use the previous thread count as the default
        DEFAULT_PARAMS["threads"] = nthreads[-2]
        do_all = "all" in args.plots
        if "size" in args.plots or do_all: plot_size(rows, f"{args.output_dir}/setbench", args.format)
        if "update" in args.plots or do_all: plot_update(rows, f"{args.output_dir}/setbench", args.format)
        if "geomean" in args.plots or do_all: plot_geomean(rows, f"{args.output_dir}/setbench", args.format)
        if "threads" in args.plots or do_all: plot_threads(rows, f"{args.output_dir}/setbench", args.format)
        if "trackers" in args.plots or do_all: plot_trackers(rows, f"{args.output_dir}/setbench", args.format)

if __name__ == "__main__":
    main()
