GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    curve25519.go
)

GO_XTEST_SRCS(
    curve25519_test.go
    vectors_test.go
)

END()

RECURSE(
    gotest
)
