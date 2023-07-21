package strawberry

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	"go.ytsaurus.tech/yt/go/yson"
)

// Controller encapsulates particular application business logic, in particular:
// how operations should be started, which files to bring with them, how to check liveness, etc.
type Controller interface {
	// Prepare builds all necessary operation spec fields.
	Prepare(ctx context.Context, oplet *Oplet) (
		spec map[string]any,
		description map[string]any,
		annotation map[string]any,
		err error)

	// Family returns short lowercase_with_underscore identifier which is included to all vanilla
	// operation annotations started by this strawberry controller. This allows efficient operation
	// filtering using YT list_operations API.
	Family() string

	ParseSpeclet(specletYson yson.RawValue) (parsedSpeclet any, err error)

	// UpdateState updates the controller's state.
	// Returns true if the state has been changed and all oplets should be restarted.
	UpdateState() (changed bool, err error)

	// DescribeOptions returns human-readable descriptors for controller-related speclet options.
	// Some options can be missing in the result if they are not intended to be visible through user interfaces.
	//
	// Given speclet should have suitable type for the specific controller.
	// Otherwise, DescribeOptions may panic.
	DescribeOptions(speclet any) []OptionGroupDescriptor
}

type ControllerFactory struct {
	Factory func(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) Controller
	Config  yson.RawValue
}
