# Building the query tracker image from source

Scripts in this folder allow building query tracker image.

Example:

1. Building artifacts for yql agent. They will be placed in `YQL_BUILD_PATH`.

    ```bash
    export YTSAURUS_SOURCE_PATH="$HOME/ytsaurus"
    export YQL_BUILD_PATH="build_yql"
    export BUILD_FLAGS="--thinlto -DCGO_ENABLED=0 -DCFLAGS=-fsized-deallocation --build=relwithdebinfo"

    ./build_yql_agent.sh
    ```

2. Building artifacts for query tracker and creating a docker image.

    ```bash
    ya package package.json
    ```

    - It assumes that yql artifacts are placed in `yql_build` directory
    - It uses fixed build flags defined in `yt/docker/ya-build/base/package.json` to build other artifacts (ytserver-all, python binaries)
    - Other arguments can adjust behaviour, for example `--docker-registry my.private.registry --docker-repository my.private.repository --custom-version my-custom-version`
