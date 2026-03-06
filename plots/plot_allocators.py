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
    "btree_lck":      "B-Tree",
    "hash_block_lck": "Hash-Block",
    "leaftree_lck":   "Leaf-Tree",
    "skiplist_lck":   "Skip-List",
    "arttree_lck":    "ART-Tree",
    "list_lck":       "Linked-List",
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

FIG_CONFIGS = {
    "figsize": (2.4, 1.8),
    "linewidth": 2,
    "markersize": 3.5,
    "xlabel_fontsize": 8,
    "ylabel_fontsize": 8,
    "xtick_fontsize": 7,
    "ytick_fontsize": 7,
    "legend_fontsize": 6,
    "title_fontsize": 8,
    "legend_ncols": len(ALLOC_PALETTE)/3,
    "dpi": 300,
    "pad_inches": 0.015,
    "xtick_end_margin": 0.1,
}

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

    l, r = ax.get_xlim()
    margin = FIG_CONFIGS["xtick_end_margin"]
    ax.set_xlim(l - margin, r + margin)

    ax.set_ylim(bottom=0)

    if not paper_print:
        ax.legend(
            bbox_to_anchor=(0.5, -0.5),
            frameon=True,
            fontsize=FIG_CONFIGS["legend_fontsize"],
            ncols=FIG_CONFIGS["legend_ncols"],
            loc="center",
            alignment="center"
        )

    else:
        plt.tight_layout()


# -- Parser -------------------------------------------------------------------
def parse_flock(path):
    rows = []
    crashes = []
    crash_re = re.compile(r"#\s*CRASH:\s*(\w+)\s+alloc=(\w+)\s+u=(\d+)\s+n=(\d+)")
    row_re   = re.compile(
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


# -- Plot 1: Throughput vs key_size (100% writes) -----------------------------
def plot_size(rows, out_dir, fmt):
    target_update = 100
    #which data structures to show for the paper
    paper_ds = ["skiplist_lck", "leaftree_lck", "hash_block_lck"]

    data = [r for r in rows if r["update"] == target_update and r["gmean"] > 0]
    dss = sorted(set(r["ds"] for r in data))

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

    return

# -- Main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Plot deqalloc experiments')
    parser.add_argument('-i', '--input_dir', type=str,
                       help='Path to directory containing timing files')
    parser.add_argument('-o', '--output-dir', type=str, default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--plots', nargs='+',
                       choices=['geomean',
                                'varying',
                                'range',
                                'combined',
                                'tpcc',
                                'nontrivial',
                                'ablation',
                                'machines',
                                'paper',
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

    rows, crashes = parse_flock(f"{args.input_dir}/flock_allocators")
    print(f"  {len(rows)} data rows, {len(crashes)} crash records")
    print(f"Saving plots to: {args.output_dir}/\n")

    plot_size(rows, args.output_dir, args.format)

if __name__ == "__main__":
    main()
