GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    encoding.go
)

GO_XTEST_SRCS(encoding_test.go)

END()

RECURSE(
    gotest
    gzip
    proto
    # yo
)
