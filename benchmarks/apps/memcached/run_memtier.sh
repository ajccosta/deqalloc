#!/usr/bin/env bash
# distributed_memtier.sh
#
# Starts a Redis/Memcached server on a remote machine via SSH, prefills it,
# then launches memtier_benchmark on multiple client machines in parallel.
# A FIFO-based barrier ensures every client starts the timed workload at
# exactly the same moment.
#
# Usage: just edit the "Configuration" section and run:
#   ./distributed_memtier.sh
#
set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

nkeys=10000000
valuesz=1
percreads=5
nthreads=$(nproc)
nclientspthread=30
duration=10
pipelinesz=100
server_type=memcache_text           # redis | memcache_text

server_machine="larochette-6"
client_machines=(
    "larochette-4"
    "larochette-5"
)

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"
BARRIER_TIMEOUT=60          # seconds to wait for all clients to reach barrier

# ─── Derived (don't edit below) ───────────────────────────────────────────────

if [[ "$server_type" == "redis" ]]; then port=6379; else port=11211; fi

num_clients=${#client_machines[@]}
workload="$((100 - percreads)):$percreads"
server_config="-s $server_machine -p $port -P $server_type -t $nthreads"
workload_config="--pipeline=$pipelinesz --key-maximum=$nkeys -c $nclientspthread -d $valuesz --key-prefix="

# ─── Logging ──────────────────────────────────────────────────────────────────

log()  { printf "\033[34m[%s]\033[0m %s\n"         "$(date '+%H:%M:%S')" "$*" >&2; }
ok()   { printf "\033[32m[%s]\033[0m %s\n"         "$(date '+%H:%M:%S')" "$*" >&2; }
warn() { printf "\033[33m[%s]\033[0m %s\n"         "$(date '+%H:%M:%S')" "$*" >&2; }
die()  { printf "\033[31m[%s] ERROR:\033[0m %s\n"  "$(date '+%H:%M:%S')" "$*" >&2; exit 1; }

# ─── Cleanup ──────────────────────────────────────────────────────────────────

cleanup() {
    log "Stopping $server_type on $server_machine..."
    # shellcheck disable=SC2029
    ssh $SSH_OPTS "$server_machine" \
        "pkill redis-server 2>/dev/null; pkill memcached 2>/dev/null; true" \
    || warn "Server cleanup failed (may already be down)"
}
trap cleanup EXIT

# ─── 1. Start server ──────────────────────────────────────────────────────────

start_server() {
    log "Starting $server_type on $server_machine..."
    if [[ "$server_type" == "redis" ]]; then
        # shellcheck disable=SC2029
        ssh $SSH_OPTS "$server_machine" "
            pkill redis-server 2>/dev/null || true
            sleep 0.5
            redis-server --daemonize yes --port $port --save '' --loglevel warning
        "
    else
        # shellcheck disable=SC2029
        ssh $SSH_OPTS "$server_machine" "
            pkill memcached 2>/dev/null || true
            sleep 0.5
            echo "LD_PRELOAD=/home/acosta/deqalloc/build/allocators/lib${allocator}.so /home/acosta/deqalloc-rebuttal/fleec/Codigo/fleec/src/fleec -d -p $port -c $(($nclientspthread * $nthreads * $num_clients * 2)) -t $(nproc) -m $((1024 * 1000))" > test
            #LD_PRELOAD=/home/acosta/deqalloc/build/allocators/lib${allocator}.so /home/acosta/deqalloc-rebuttal/fleec/Codigo/fleec/src/fleec -d -p $port -c $(($nclientspthread * $nthreads * $num_clients * 2)) -t $(nproc) -m $((1024 * 1000))
        "
            #memcached -d -p $port -c $(($nclientspthread * $nthreads * $num_clients * 2)) -t $(nproc) -m $((1024 * 1000))
    fi
}

# ─── 2. Poll until server port is accepting connections ───────────────────────

wait_for_server() {
    log "Waiting for $server_type on $server_machine:$port..."
    local i
    for (( i=0; i<30; i++ )); do
        nc -z "$server_machine" "$port" 2>/dev/null && { ok "Server is up"; return 0; }
        sleep 1
    done
    die "Server did not become ready within 30 s"
}

# ─── 3. Prefill ───────────────────────────────────────────────────────────────

prefill() {
    log "Prefilling $nkeys keys from ${client_machines[0]}..."
    # shellcheck disable=SC2029
    ssh $SSH_OPTS "${client_machines[0]}" "
        memtier_benchmark $server_config $workload_config \
            --ratio=1:0 -n allkeys --key-pattern=P:P --multi-key-get=100 > /dev/null
    "
    ok "Prefill done"
}

# ─── 4. Distributed benchmark with barrier ────────────────────────────────────
#
# Barrier mechanism (pure bash, no shared filesystem needed):
#
#   Coordinator side                    Client side
#   ─────────────────                   ───────────
#   mkfifo go_N                         (stdin  = go_N  FIFO)
#   exec {fd}<> go_N  ← O_RDWR open    (stderr = sig_N file)
#     (non-blocking; keeps write-end    (stdout = out_N file)
#      open so SSH read-open succeeds)
#   ssh ... < go_N &                    printf 'BENCH_READY\n' >&2
#                                       read -r _go          ← blocks here
#   grep sig_N for BENCH_READY          ...waiting...
#   echo GO >&$fd   → unblocks read     exec memtier_benchmark ...
#   exec {fd}>&-   (close write-end)
#
# All "echo GO" writes happen in a tight loop before any client's read()
# returns, giving the tightest possible simultaneous start across machines.
# ──────────────────────────────────────────────────────────────────────────────

run_benchmark() {
    log "Launching $num_clients clients with barrier sync..."
    local tmpdir; tmpdir=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '$tmpdir'" RETURN

    local pids=() gofds=()

    # This script runs on each remote client:
    #   1. Signals the coordinator it has started and is ready.
    #   2. Blocks on stdin until the coordinator writes "GO".
    #   3. Hands off to memtier via exec (avoids a subshell).
    local client_script="
        printf 'BENCH_READY\n' >&2
        read -r _go
        exec memtier_benchmark $server_config $workload_config \
            --ratio=$workload --test-time=$duration --key-pattern=Z:Z
    "

    for i in "${!client_machines[@]}"; do
        local client="${client_machines[$i]}"
        local fifo="$tmpdir/go_$i"
        mkfifo "$fifo"

        # Open FIFO in O_RDWR on the coordinator so the SSH process's
        # O_RDONLY open (< "$fifo") does not block waiting for a writer.
        local gofd
        exec {gofd}<>"$fifo"
        gofds+=("$gofd")

        # shellcheck disable=SC2029
        ssh $SSH_OPTS "$client" "$client_script" \
            < "$fifo" \
            > "$tmpdir/out_$i" \
            2> "$tmpdir/sig_$i" &
        pids+=($!)
        log "  Launched $client (pid ${pids[-1]})"
    done

    # ── Wait for every client to reach the barrier ────────────────────────────
    log "Waiting for all $num_clients clients (timeout: ${BARRIER_TIMEOUT}s)..."
    local deadline=$(( $(date +%s) + BARRIER_TIMEOUT ))
    for i in "${!client_machines[@]}"; do
        while ! grep -q "BENCH_READY" "$tmpdir/sig_$i" 2>/dev/null; do
            (( $(date +%s) <= deadline )) \
                || die "Timeout waiting for ${client_machines[$i]} to reach barrier"
            kill -0 "${pids[$i]}" 2>/dev/null \
                || die "${client_machines[$i]}: SSH process exited before reaching barrier"
            sleep 0.1
        done
        ok "  ${client_machines[$i]} ready"
    done

    # ── Release all clients ───────────────────────────────────────────────────
    # Writing GO to each FIFO in rapid succession — all clients unblock within
    # the same millisecond window.
    ok "All clients ready — GO!"
    for i in "${!client_machines[@]}"; do
        echo "GO" >&"${gofds[$i]}"
    done

    # Close coordinator's write-ends so clients see EOF if they read again
    for i in "${!client_machines[@]}"; do
        local fd="${gofds[$i]}"
        exec {fd}>&-
    done

    # ── Collect exit statuses ─────────────────────────────────────────────────
    local all_ok=1
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            ok "  ${client_machines[$i]} finished"
        else
            warn "  ${client_machines[$i]} exited with error"
            all_ok=0
        fi
    done

    # ── Print per-client results ──────────────────────────────────────────────
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf "  RESULTS  (%d clients × %d threads × %d conns/thread)\n" \
        "$num_clients" "$nthreads" "$nclientspthread"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    for i in "${!client_machines[@]}"; do
        printf "\n── %s ──\n" "${client_machines[$i]}"
        if grep -q "ALL STATS" "$tmpdir/out_$i" 2>/dev/null; then
            grep "ALL STATS" -A 7 "$tmpdir/out_$i"
        else
            warn "No ALL STATS found for ${client_machines[$i]} — raw output:"
            cat "$tmpdir/out_$i" >&2
        fi
    done

    (( all_ok )) || exit 1
}

# ─── Main ─────────────────────────────────────────────────────────────────────

start_server
wait_for_server
prefill
run_benchmark
