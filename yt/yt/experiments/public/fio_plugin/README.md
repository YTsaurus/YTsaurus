`ya make . -DFIO_SRC=~/src/fio`

Options:
fio -enghelp=./libfio-ytsaurus.so

~/src/fio/fio -ioengine=./libfio-ytsaurus.so -name test -filename=//tmp/test -bs=1m -size=1G

put /usr/local/lib/fio/fio-ytsaurus.so
run fio -ioengine=ytsaurus
