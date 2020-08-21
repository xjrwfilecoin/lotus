#!/bin/bash
if [ $# != 1 ] ; then
	echo "usage make_lotus.sh local/remote/raw"
fi
trap 'rm -f "$TMPFILE"' EXIT
#生成临时目录
#TMPFILE=$(mktemp) || exit 1
##git clone 相关的库
#cd $TMPFILE
##rust-filecoin-proofs-api
#git clone https://github.com/plotozhu/rust-filecoin-proofs-api
#cd rust-filecoin-proofs-api
#git checkout local
#cd ..
## rust-files
#mkdir   ./rust-file-local
#cd rust-file-local
rm -fr ./extern/filecoin-ffi
mkdir -p ./extern/filecoin-ffi

if [ $1 == "local" ];then
  echo "using local ffi"
	cp -fr  extern/xjrw-ffi/* extern/filecoin-ffi/
elif [ $1 == "remote" ]; then
  echo "using remote ffi "
	cp -fr extern/DBC-filecoin-ffi/* extern/filecoin-ffi/
elif [ $1 == "raw" ]; then
  echo "use origin"
else
	echo "param should be remote / local or raw"
	exit
fi

(env 'RUSTFLAGS=-C target-cpu=native -g' FFI_BUILD_FROM_SOURCE=1 make clean all lotus-bench)

mkdir -p ~/lotus-$1
cp lotus lotus-* ~/lotus-$1

