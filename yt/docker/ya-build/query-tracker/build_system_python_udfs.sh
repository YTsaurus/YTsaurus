#!/usr/bin/env bash

# This script builds system python udfs.
# It is a separate script because it depends on environment(local pythons).

# Required environment variables:
# YQL_BUILD_PATH - path to the build directory. All artifacts will be placed here.
# BUILD_FLAGS - flags to pass to ya make when building.

set -eux
shopt -s expand_aliases

export YQL_BUILD_PATH=$(realpath $YQL_BUILD_PATH)

mkdir -p $YQL_BUILD_PATH

for version in 3.8 \
               3.9 \
               3.10 \
               3.11 \
               3.12
do
  udf_suffix=$(echo "$version" | tr . _)
  udf_name="python${udf_suffix}"
  python_name="python${version}"

  # USE_LOCAL_PYTHON leads to a failure due to a bug in ya.
  # /usr/include/pythonX/pyconfig.h cannot include ARCH/pythonX/pyconfig.h without -I/usr/include
  # Adding symlink /usr/include/pythonX/ARCH/pythonX -> /usr/include/ARCH/pythonX

  INCLUDES=$(${python_name}-config --includes | sed -n 's/^-I\(\S\+\) .*/\1/p')
  ARCH=$(dpkg-architecture -q DEB_BUILD_MULTIARCH)
  if [ ! -e ${INCLUDES}/${ARCH} ]; then
    mkdir -p ${INCLUDES}/${ARCH}
    ln -s $(dirname ${INCLUDES})/${ARCH}/$(basename ${INCLUDES}) ${INCLUDES}/${ARCH};
  fi

  python3 $YTSAURUS_SOURCE_PATH/ya make $YTSAURUS_SOURCE_PATH/yql/essentials/udfs/common/python/system_python/${udf_name} \
    -T ${BUILD_FLAGS} --ignore-recurses -DSTRIP=yes -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON -DPYTHON_CONFIG=${python_name}-config -DPYTHON_BIN=${python_name} --output $YQL_BUILD_PATH
  if [[ "$BUILD_FLAGS" != *"--bazel-remote-put"* ]]; then
    strip --remove-section=.gnu_debuglink $YQL_BUILD_PATH/yql/essentials/udfs/common/python/system_python/${udf_name}/libsystem${udf_name}_udf.so
  fi
done
