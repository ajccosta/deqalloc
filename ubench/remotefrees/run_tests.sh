#!/bin/bash

#parameters
nthreads=$(( $(nproc) - 1 ))
repeat=5
batchsizes="500000"
rfps="0 5 25 50 95" #remote free percentagees
#allocators="deqalloc hoard jemalloc mimalloc"
allocators="deqalloc jemalloc mimalloc"

dir=$(dirname $(realpath $0))
results_file=$dir/rf_experiment_$(echo $allocators | tr ' ' '-')-${nthreads}thr-${repeat}r_$(date '+%d-%h-%Y_%H-%M-%S')

for batchsize in $(echo -e "$batchsizes"); do
  for rfp in $(echo -e "$rfps"); do
    printf "batch size: $batchsize, remote frees: $rfp%%\n" | tee -a $results_file;
    for allocator in $(echo -e "$allocators"); do
      printf "$allocator " | tee -a $results_file;
      res=$(LD_PRELOAD=${dir}/lib${allocator}.so ./free-benchmark -t $nthreads -r $repeat -p $rfp -n $batchsize -s 500000000 -b 32);
      res=$(echo -e "$res" | grep -E "Average frees/sec:" | sed 's/Average frees\/sec: //g')
      printf "$res\n" | tee -a $results_file;
    done
  done
done
