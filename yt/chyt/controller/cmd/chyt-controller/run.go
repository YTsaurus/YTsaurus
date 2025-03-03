package main

import (
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/jupyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/livy"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/internal/tryt"
)

var runCmd = &cobra.Command{
	Use: "run",
	Run: wrapRun(doRun),
}

var (
	flagForceFlush bool
)

func init() {
	runCmd.PersistentFlags().BoolVar(&flagForceFlush, "force-flush", false,
		"if set, all operation states will be forcefully flushed after attaching")
	rootCmd.AddCommand(runCmd)
}

func doRun() error {
	var config app.Config
	loadConfig(flagConfigPath, &config)
	options := app.Options{
		LogToStderr: flagLogToStderr,
	}

	cfs := map[string]strawberry.ControllerFactory{}

	// CHYT controller is always present.

	chytConfig, ok := config.Controllers["chyt"]
	if !ok {
		chytConfig = config.Controller
	}
	chytFactory := strawberry.ControllerFactory{
		Ctor:   chyt.NewController,
		Config: chytConfig,
	}
	cfs["chyt"] = chytFactory

	// JUPYT controller is optional.

	if jupytConfig, ok := config.Controllers["jupyt"]; ok {
		jupytFactory := strawberry.ControllerFactory{
			Ctor:          jupyt.NewController,
			Config:        jupytConfig,
			ExtraCommands: jupyt.AllCommands,
		}
		cfs["jupyt"] = jupytFactory
	}

	// SPYT Livy controller is optional.

	if livyConfig, ok := config.Controllers["livy"]; ok {
		livyFactory := strawberry.ControllerFactory{
			Ctor:          livy.NewController,
			Config:        livyConfig,
			ExtraCommands: []api.CmdDescriptor{},
		}
		cfs["livy"] = livyFactory
	}

	// TRYR Transfer controller is optional

	if trytConfig, ok := config.Controllers["tryt"]; ok {
		cfs["tryt"] = strawberry.ControllerFactory{
			Ctor:          tryt.NewController,
			Config:        trytConfig,
			ExtraCommands: tryt.AllCommands,
		}
	}

	a := app.New(&config, &options, cfs)
	a.Run(make(chan struct{}) /*stopCh*/)
	panic("unreachable")
}
