
## deqalloc - the memory stealing allocator

To compile, run the following commands:

```
mkdir build
cd build
cmake ..
make -j
```

To substitute deqalloc as your malloc implementation with LD\_PRELOAD, do:
```
LD_PRELOAD=./libdeqalloc.so <your command here>
```
