PY3TEST()

SET(YT_SPLIT_FACTOR 30)

ENV(YT_TEST_FILTER=XLARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:46
        ram:62
        ram_disk:12
    )
ELSE()
    REQUIREMENTS(
        cpu:38
        ram:24
    )
ENDIF()

END()
