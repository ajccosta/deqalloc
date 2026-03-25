#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
from matplotlib.ticker import MaxNLocator
from matplotlib.lines import Line2D
import sys
import os

if len(sys.argv) != 2:
    print("Usage: python3 plot_perf.py <file>")
    sys.exit(1)

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
    "2geibr_df"    : "ibr_af",
    "debra_df"     : "debra_af",
    "he_df"        : "he_af",
    "ibr_hp_df"    : "hp_af",
    "ibr_rcu_df"   : "ebr_af",
    "nbr_df"       : "nbr_af",
    "nbrplus_df"   : "nbr+_af",
    "qsbr_df"      : "qsbr_af",
    "wfe_df"       : "wfe_af",
    "token4_df"    : "token",
}

# ---------- Load & aggregate ----------
filename = sys.argv[1]
df = pd.read_csv(filename)

# 1. Clean the percentage column (remove '%' and cast to float)
df["remote freeing (%)"] = df["remote freeing (%)"].str.rstrip('%').astype(float)

# 2. Rename 'tracker' to 'smr' to match the plotting logic
df.rename(columns={"tracker": "smr"}, inplace=True)

# Average repetitions
df = (
    df.groupby(["allocator", "ds", "smr"], as_index=False)
      .agg(value=("remote freeing (%)", "mean"))
)

# Order SMRs explicitly (important for stable layout)
# 3. Updated 'hp' to 'ibr_hp' and 'rcu' to 'ibr_rcu' to match the new CSV format
smr_order = [
    "2geibr", "2geibr_df",
    "debra", "debra_df",
    "he", "he_df",
    "ibr_hp", "ibr_hp_df",
    "ibr_rcu", "ibr_rcu_df",
    "nbr", "nbr_df",
    "nbrplus", "nbrplus_df",
    "qsbr", "qsbr_df",
    "wfe", "wfe_df",
    "token4",
]

df["smr"] = pd.Categorical(df["smr"], smr_order, ordered=True)

# Data structures to plot
ds_list = df["ds"].unique()
allocators = df["allocator"].unique()

# ---------- Plot styling ----------
plt.rcParams.update({
    "font.size": 11,
    #"axes.grid": True,
    "grid.linestyle": "--",
    "grid.alpha": 0.5,
    "axes.formatter.use_mathtext": True,
})

mult = 1
fig, axes = plt.subplots(2, 1, sharex=True, figsize=(12*mult, 2.5*mult))

x = np.arange(len(smr_order))
width = 0.85 / len(ds_list)

ds_hatches = ['..', 'oo', 'OO', 'O.']
allocator_hatches = [None, None]

colors = ["orangered", "royalblue", "forestgreen", "gold"]

# repeat color scheme as necessary
colors = np.array([colors] * (int(len(ds_list) / 2 + 0.5))).flatten()

# ---------- Labels ----------
allocator_handles = [
    Patch(facecolor="white", edgecolor="black", hatch=allocator_hatches[i], label=a)
    for i, a in enumerate(allocators)
]

ds_handles = [
    Patch(facecolor=colors[i], edgecolor="black", hatch=ds_hatches[i % len(ds_hatches)], label=DS_LABELS.get(ds, ds))
    for i, ds in enumerate(ds_list)
]

# ---------- Bars ----------
for i, ds in enumerate(ds_list):
    for j, allocator in enumerate(allocators):
        ax_ = axes[j]
        all_df = df[df["allocator"] == allocator]
        y = (
            all_df[all_df["ds"] == ds]
            .set_index("smr")
            .reindex(smr_order)["value"]
        )
        ax_.bar(
            x + i * width,
            y,
            width,
            hatch=ds_hatches[i % len(ds_hatches)],
            edgecolor="black",
            alpha=1,
            color=colors[i],
        )

# ---------- Axes ----------

# replace some strings for coherency sake
for i in range(len(smr_order)):
    smr_order[i] = smr_order[i].replace(smr_order[i], TRACKER_LABELS.get(smr_order[i]))
    smr_order[i] = smr_order[i].replace("2geibr", "ibr")
    smr_order[i] = smr_order[i].replace("plus", "+")
    smr_order[i] = smr_order[i].replace("_df", "_af")

ax = axes[0]
ax.set_xticks(x + width * (len(ds_list) - 1) / 2)
ax.set_xticklabels(smr_order, rotation=45, ha="right")
ax.ticklabel_format(axis="y", style="plain", scilimits=(0, 0))

legs = []
legs.append(ax.legend(
    handles=ds_handles,
    frameon=True,
    fontsize=9,
    ncol=len(ds_list),
    loc="upper left",
    alignment="left"
))

# 1. Find the highest value across ALL data
global_max_y = df["value"].max()

# 2. Add a 10% buffer so the tallest bar looks nice
y_limit = global_max_y * 1.1

for j, allocator in enumerate(allocators):
    ax_ = axes[j]
    handles = [Line2D([], [], linestyle="none", label=alloc) for alloc in [allocator]]
    legs.append(ax_.legend(
        handles=handles,
        handlelength=0,
        handletextpad=0,
        frameon=True,
        fontsize=9,
        ncol=1,
        loc="upper right",
        alignment="left",
    ))
    ax_.set_ylabel("runtime %")
    # margin extend
    ax.set_xlim(x.min() - width, x.max() + width * len(ds_list))
    ax.margins(x=0)
    # grid
    maxy = int((int(ax_.get_ylim()[1]) / 10)) * 10
    
    # Prevent divide by zero error if maxy is 0
    if maxy > 0:
        ax_.yaxis.set_major_locator(MultipleLocator(maxy / 2))
        
    ax_.grid(True, which="major", axis="y")
    ax_.set_ylim(0, y_limit)

legs.pop()
for l in legs:
    ax.add_artist(l)

plt.tight_layout()
os.makedirs(f"plots/", exist_ok=True)
plt.savefig("plots/perf_analysis.pdf")
