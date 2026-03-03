import struct, sys, os, bisect
import matplotlib.pyplot as plt

# Define the format string for struct.unpack
# '<' indicates little-endian byte order
# 'I' for uint32_t (4 bytes)
# 'B' for uint8_t (1 byte)
# 'Q' for uint64_t (8 bytes)
entry_format = '<IBQ'

size_classes = [
    8, 16, 32, 48, 64, 80, 96, 112, 128,
    160, 192, 224, 256, 320, 384, 448, 512,
    640, 768, 896, 1024,   
    1280, 1536, 1792, 2048,
    2560, 3072, 3584, 4096,
    5 * 1024, 6 * 1024, 7 * 1024, 8 * 1024,
    10 * 1024, 12 * 1024, 14 * 1024, 16 * 1024,
    20 * 1024, 24 * 1024, 28 * 1024, 32 * 1024,
    40 * 1024, 48 * 1024, 56 * 1024, 64 * 1024,
    80 * 1024, 96 * 1024, 112 * 1024, 128 * 1024,
    160 * 1024, 192 * 1024, 224 * 1024, 256 * 1024,
    320 * 1024, 384 * 1024, 448 * 1024, 512 * 1024,
    640 * 1024, 768 * 1024, 896 * 1024, 1024 * 1024,
    1280 * 1024, 1536 * 1024, 1792 * 1024,
    2 * 1024 * 1024, 2560 * 1024, 3 * 1024 * 1024,
    3584 * 1024, 4 * 1024 * 1024, 5 * 1024 * 1024,
    6 * 1024 * 1024, 7 * 1024 * 1024, 8 * 1024 * 1024,
    10 * 1024 * 1024, 12 * 1024 * 1024, 14 * 1024 * 1024,
    16 * 1024 * 1024, 20 * 1024 * 1024, 24 * 1024 * 1024,
    28 * 1024 * 1024, 32 * 1024 * 1024, 40 * 1024 * 1024,
    48 * 1024 * 1024, 56 * 1024 * 1024, 64 * 1024 * 1024
]

def get_size_class(size):
    idx = bisect.bisect_left(size_classes, size)
    if idx == len(size_classes):
        raise ValueError("Size too large for available size classes")
    return size_classes[idx]

MALLOC=0
FREE=1

#function partially written by ChatGPT
update_step = 0.01 #percentage points
previous_prog_perc = -update_step
progress = 0
def progress_bar(total, length=50):
    global progress, previous_prog_perc
    progress += 1
    percent = progress / total
    if percent >= (previous_prog_perc + update_step):
        previous_prog_perc = percent
        filled_length = int(length * percent)
        bar = "█" * filled_length + "-" * (length - filled_length)
        sys.stdout.write(f"\r|{bar}| {percent:.0%}")
        sys.stdout.flush()
        if int(percent) == 1: #reset
            previous_prog_perc = -update_step
            progress = 0

# Define a function to read and parse the binary file
def read_entries(filename):
    entries = []
    entry_sz = struct.calcsize(entry_format)
    file_sz = os.path.getsize(filename)
    n_entries = file_sz / entry_sz
    with open(filename, 'rb') as f:
        print(f"Reading {filename}")
        while True:
            data = f.read(entry_sz)
            progress_bar(n_entries)
            if not data:
                break
            #pad with zeroes
            data = data.ljust(entry_sz, b'\00')
            sz, typ, ts = struct.unpack(entry_format, data)
            #adjust size class for mallocs
            sz = get_size_class(sz) if typ == MALLOC else sz
            entries.append((sz, typ, ts))
    return entries

#sorts a and b by a
def sort2lists(a, b):
    a, b = zip(*sorted(zip(a, b)))
    return list(a), list(b)

# Define a function to plot the allocations
def plot_allocations(entries):
    allocs=dict()
    total_prog = len(entries)
    print(f"\nCreating indexes")
    for sz, typ, ts in entries:
        progress_bar(total_prog)
        if sz not in allocs:
            allocs[sz] = dict()
        if ts not in allocs[sz]:
            allocs[sz][ts] = [0,0]#malloc,free
        if typ == MALLOC: #malloc
            allocs[sz][ts][0] += 1
        else:
            allocs[sz][ts][1] += 1

    keys = sorted(allocs.keys())

    keys = [k for k in keys if len(allocs[k]) > 5]
    nkeys = len(keys)
    fig, axs = plt.subplots(nkeys, 1, figsize=(10, 5*nkeys))

    i=0
    for k in keys:
        timestamps = list(allocs[k].keys())
        ax = axs[i]
        i += 1

        timestamps, allocs_sorted = sort2lists(timestamps, allocs[k].values())
        mallocs, frees = zip(*allocs_sorted)
        mallocs = list(mallocs)
        frees = list(frees)

        ax.set_title(f"size class: {k}")
        ax.set_xlabel(f"timestamp")
        ax.set_ylabel(f"number of allocations")
        ax.plot(timestamps, mallocs, '.', label="mallocs", color='blue', alpha=0.7)
        ax.plot(timestamps, frees, '.', label="frees", color='red', alpha=0.7)
        ax.legend()

        #plt.figure(figsize=(10, 6))
        #plt.xlabel('Timestamp')
        #plt.ylabel('Number of allocations')
        #plt.title('Memory Allocations Over Time')
        #plt.grid(True)
        #plt.plot(timestamps, mallocs, label="mallocs", color='blue', marker='o')
        #plt.plot(timestamps, frees, label="frees", color='red', marker='o')
        #plt.legend()
        #plt.savefig(f"test{k}.png")
        #plt.clf()
        #plt.close()

    fig.tight_layout()
    fig.savefig("combined.png", dpi=300) 

# Main function
def main():
    filename = 'test.out'  # Replace with your actual file path
    entries = read_entries(filename)
    plot_allocations(entries)
    print()

if __name__ == '__main__':
    main()
