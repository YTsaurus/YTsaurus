package agent

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

// Config contains strawberry-specific configuration.
type Config struct {
	// Root points to root directory with operation states.
	Root ypath.Path `yson:"root"`

	// PassPeriod defines how often agent performs its passes.
	PassPeriod *yson.Duration `yson:"pass_period"`
	// CollectOperationsPeriod defines how often agent collects running operations.
	CollectOperationsPeriod *yson.Duration `yson:"collect_operations_period"`
	// RevisionCollectPeriod defines how often agent collects Cypress node revisions via batch ListNode.
	RevisionCollectPeriod *yson.Duration `yson:"revision_collect_period"`

	// HealthCheckerToleranceFactor is a maximum period factor before a health checker
	// considers the state as `expired`. E.g. if the pass period is 5s and the factor is 2.0,
	// the health checker will consider the state valid for 10s.
	HealthCheckerToleranceFactor *float64 `yson:"health_checker_tolerance_factor"`

	// Stage of the controller, e.g. production, prestable, etc.
	Stage string `yson:"stage"`

	// RobotUsername is the name of the robot from which all the operations are started.
	// It is used to check permission to the pool during setting "pool" option.
	RobotUsername string `yson:"robot_username"`

	// PassWorkerNumber is the number of workers used to process oplets.
	PassWorkerNumber *int `yson:"pass_worker_number"`

	// DefaultNetworkProject is the default network project used for oplets.
	DefaultNetworkProject *string `yson:"default_network_project"`

	// ClusterURLTemplate is a template executed via text/template library to get cluster URL.
	// Cluster URL is used for generating "fancy" links for debug purposes only.
	// If template parsing or execution fails, panic is called.
	// Available template parameters: Proxy.
	// E.g. "https://example.com/{{.Proxy}}"
	ClusterURLTemplate string `yson:"cluster_url_template"`

	// AssignAdministerToCreator determines whether the operation creator
	// should be granted the `administer` right to the corresponding ACO.
	AssignAdministerToCreator *bool `yson:"assign_administer_to_creator"`
}

const (
	DefaultPassPeriod                   = yson.Duration(5 * time.Second)
	DefaultCollectOperationsPeriod      = yson.Duration(time.Minute)
	DefaultRevisionCollectPeriod        = yson.Duration(5 * time.Second)
	DefaultHealthCheckerToleranceFactor = 2.0
	DefaultPassWorkerNumber             = 1
	DefaultAssignAdministerToCreator    = true
)

func (c *Config) PassPeriodOrDefault() yson.Duration {
	if c.PassPeriod != nil {
		return *c.PassPeriod
	}
	return DefaultPassPeriod
}

func (c *Config) CollectOperationsPeriodOrDefault() yson.Duration {
	if c.CollectOperationsPeriod != nil {
		return *c.CollectOperationsPeriod
	}
	return DefaultCollectOperationsPeriod
}

func (c *Config) RevisionCollectPeriodOrDefault() yson.Duration {
	if c.RevisionCollectPeriod != nil {
		return *c.RevisionCollectPeriod
	}
	return DefaultRevisionCollectPeriod
}

func (c *Config) HealthCheckerToleranceFactorOrDefault() float64 {
	if c.HealthCheckerToleranceFactor != nil {
		return *c.HealthCheckerToleranceFactor
	}
	return DefaultHealthCheckerToleranceFactor
}

func (c *Config) PassWorkerNumberOrDefault() int {
	if c.PassWorkerNumber != nil {
		return *c.PassWorkerNumber
	}
	return DefaultPassWorkerNumber
}

func (c *Config) AssignAdministerToCreatorOrDefault() bool {
	if c.AssignAdministerToCreator != nil {
		return *c.AssignAdministerToCreator
	}
	return DefaultAssignAdministerToCreator
}
