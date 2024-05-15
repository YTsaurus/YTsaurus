PY3_LIBRARY()

INCLUDE(spark_distrib.inc)

PEERDIR(
    yt/python/yt/wrapper
    yt/spark/spark-over-yt/spyt-package/src/main/python
    contrib/python/pyspark/${SPARK_VERSION}
    contrib/python/pyarrow
    contrib/python/PyYAML
)

FROM_SANDBOX(
    6309998289 AUTOUPDATED spyt_cluster
    OUT_NOAUTO spyt_cluster/spyt-package.zip
)

RESOURCE_FILES(
    PREFIX yt/spark/spark-over-yt/spyt-package/src/main/
    spyt_cluster/spyt-package.zip
)

END()

RECURSE(bin)
