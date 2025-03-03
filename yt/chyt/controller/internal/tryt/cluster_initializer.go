package tryt

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ClusterInitializer struct {
	ytc yt.Client
}

func (i *ClusterInitializer) InitializeCluster() error {
	return nil
}

func (i *ClusterInitializer) ACONamespace() string {
	return "tryt"
}

func NewClusterInitializer(l log.Logger, ytc yt.Client, root ypath.Path) strawberry.ClusterInitializer {
	return &ClusterInitializer{ytc: ytc}
}
