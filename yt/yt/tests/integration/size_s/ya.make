PY3TEST()

SET(YT_SPLIT_FACTOR 45)

ENV(YT_TEST_FILTER=SMALL)

INCLUDE(../YaMakeDependsBoilerplate.txt)

REQUIREMENTS(
    cpu:16
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:24
    )
ELSE()
    REQUIREMENTS(
        ram:12
    )
ENDIF()

END()

