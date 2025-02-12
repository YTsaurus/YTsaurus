DLL(fio-ytsaurus)

EXPORTS_SCRIPT(
     fio-shim.exports
)

SET(FIO_SRC /home/khlebnikov/src/fio)
SET(FIO_IOOPS_VERSION 36)

# IN_NOPARSE is broken - try to rename fio-shim.xxx into fio-shim.in.cpp
RUN_PYTHON3(
     exec.py ${CXX_COMPILER} -E -I${FIO_SRC} -DBUMP=1 -x c++ fio-shim.xxx
     IN_NOPARSE fio-shim.xxx
     IN plugin.hpp
     STDOUT fio-shim.cpp
)

CXXFLAGS(
     -Wno-unused-parameter
)

PEERDIR(
     yt/yt/core
     yt/yt/client
     yt/cpp/mapreduce/client
     yt/yt/ytlib
)

SRC(
     plugin.cpp
)

END()
