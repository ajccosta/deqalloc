#!/bin/bash
python3 plot_allocators.py -i ../timings/larochette/
python3 plot_perf.py ../timings/larochette/perf/results.csv