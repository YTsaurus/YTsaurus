GO_LIBRARY()

LICENSE(MIT)

SRCS(
    auth.go
    config.go
    load.go
)

GO_TEST_SRCS(auth_test.go)

END()

RECURSE(
    gotest
)
