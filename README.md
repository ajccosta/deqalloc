
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

## License

deqalloc is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version (LGPL-3.0-or-later). See `COPYING.LESSER` and `COPYING`.

In short: programs of any license may link deqalloc or load it with
`LD_PRELOAD`; changes to deqalloc itself must be shared under the same terms.

Some files come from other projects and keep their own notices: files derived
from Heap Layers (Emery Berger) are under the Apache 2.0 license, and
`HarrisLinkedList.h` (Correia, Ramalhete, Felber) is under the MIT license.
