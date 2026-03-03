#!/bin/bash

#build multiple allocators
#instructions mostly taken from https://github.com/daanx/mimalloc-bench/blob/master/build-bench-env.sh

mkdir -p allocators
pushd allocators

#compile jemalloc
git clone https://github.com/jemalloc/jemalloc
pushd jemalloc
./autogen.sh --enable-doc=no --enable-static=no --disable-stats
make -j 8
popd
cp $(readlink -f jemalloc/lib/libjemalloc.so) libjemalloc.so
rm -rf jemalloc

#compile mesh
git clone https://github.com/plasma-umass/mesh
pushd mesh
cmake .
make -j 8
make -j 8 #for some reason it only builds on the second attempt
popd
cp $(readlink -f mesh/build/lib/libmesh.so) libmesh.so
rm -rf mesh

#compile mimalloc
git clone https://github.com/microsoft/mimalloc.git
pushd mimalloc
git checkout master
cmake -B out/release
cmake --build out/release --parallel 8
popd
cp $(readlink -f mimalloc/out/release/libmimalloc.so) libmimalloc.so
rm -rf mimalloc

#compile hoard
git clone https://github.com/emeryberger/Hoard
pushd Hoard/src
make -j 8
popd
cp $(readlink -f Hoard/src/libhoard.so) libhoard.so
rm -rf Hoard

#compile scalloc
git clone https://github.com/cksystemsgroup/scalloc
pushd scalloc
./tools/make_deps.sh
./tools/gyp --depth=. scalloc.gyp
BUILDTYPE=Release make
popd
cp $(readlink -f scalloc/out/Release/lib.target/libscalloc.so) libscalloc.so
rm -rf scalloc

#compile tcmalloc
git clone https://github.com/google/tcmalloc
pushd tcmalloc
ORIG=""
sed -i $ORIG '/linkstatic/d' tcmalloc/BUILD
sed -i $ORIG '/linkstatic/d' tcmalloc/internal/BUILD
sed -i $ORIG '/linkstatic/d' tcmalloc/testing/BUILD
sed -i $ORIG '/linkstatic/d' tcmalloc/variants.bzl
gawk -i inplace '(f && g) {$0="linkshared = True, )"; f=0; g=0} /This library provides tcmalloc always/{f=1} /alwayslink/{g=1} 1' tcmalloc/BUILD
gawk -i inplace 'f{$0="cc_binary("; f=0} /This library provides tcmalloc always/{f=1} 1' tcmalloc/BUILD # Change the line after "This library…" to cc_binary (instead of cc_library)
gawk -i inplace '/alwayslink/ && !f{f=1; next} 1' tcmalloc/BUILD # delete only the first instance of "alwayslink"
bazel build -c opt tcmalloc
popd
cp $(readlink -f tcmalloc/bazel-bin/tcmalloc/libtcmalloc.so) libtcmalloc.so
rm -rf tcmalloc

#compile snmalloc
git clone https://github.com/microsoft/snmalloc
mkdir -p snmalloc/build
pushd snmalloc/build
env CXX=clang++ cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja libsnmallocshim.so libsnmallocshim-checks.so
popd
mv snmalloc/build/libsnmallocshim.so libsnmalloc.so
rm -rf snmalloc

#compile lockfree 
git clone https://github.com/Begun/lockfree-malloc
pushd lockfree-malloc
make -j $procs liblite-malloc-shared.so
popd
cp $(readlink -f lockfree-malloc/liblite-malloc-shared.so) liblockfree.so
rm -rf lockfree-malloc

#compile rpmalloc
git clone https://github.com/mjansson/rpmalloc
pushd rpmalloc
CC=clang-16 CXX=clang++-16 python3 configure.py
# fix build using clang-16
# see https://github.com/mjansson/rpmalloc/issues/316
sed -i 's/-Werror//' build.ninja
ninja
popd
cp rpmalloc/build/ninja/linux/release/x86-64/rpmalloc-cccf0ca/librpmalloc.so .
rm -rf rpmalloc

#compile tbbmalloc
git clone https://github.com/oneapi-src/oneTBB
pushd oneTBB
cmake -DCMAKE_BUILD_TYPE=Release -DTBB_BUILD=OFF -DTBB_TEST=OFF -DTBB_OUTPUT_DIR_BASE=bench .
make -j $procs
popd
cp $(readlink -f oneTBB/bench_release/libtbbmalloc.so) ./libtbbmalloc.so
rm -rf oneTBB

popd

