GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    common.go
    config.go
    container.go
    network.go
    node.go
    runtime.go
    secret.go
    service.go
    service_create_response.go
    service_update_response.go
    swarm.go
    task.go
)

END()

RECURSE(
    runtime
)
