#!/bin/bash

nthreads=$(( $(nproc) - 1 ))
niters=$(( 150000 * nthreads ))

printf "#segments\tthroughput (Mop/s)\tHuge pages uniform\n"
for i in $(seq 0 12); do s=$((2**i)); printf "$s\t\t$(./tlb_bench --threads $nthreads --segments $s --iterations $niters | grep Throughput | cut -d' ' -f2)\n"; done

printf "#segments\tthroughput (Mop/s)\tNo huge pages uniform\n"
for i in $(seq 0 12); do s=$((2**i)); printf "$s\t\t$(./tlb_bench --threads $nthreads --segments $s --iterations $niters --nohugepage | grep Throughput | cut -d' ' -f2)\n"; done

printf "#segments\tthroughput (Mop/s)\tHuge pages zipfian\n"
for i in $(seq 0 12); do s=$((2**i)); printf "$s\t\t$(./tlb_bench --threads $nthreads --segments $s --iterations $niters --zipf | grep Throughput | cut -d' ' -f2)\n"; done

printf "#segments\tthroughput (Mop/s)\tNo huge pages zipfian\n"
for i in $(seq 0 12); do s=$((2**i)); printf "$s\t\t$(./tlb_bench --threads $nthreads --segments $s --iterations $niters --nohugepage --zipf | grep Throughput | cut -d' ' -f2)\n"; done