package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/jupyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/internal/tryt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

var oneShotRunCmd = &cobra.Command{
	Use: "one-shot-run",
	Run: wrapRun(doOneShotRun),
}

var (
	flagCliqueAlias string
	flagSpecletPath string
	flagFamily      string
)

func init() {
	oneShotRunCmd.PersistentFlags().StringVar(&flagCliqueAlias, "alias", "", "clique alias")
	if err := oneShotRunCmd.MarkPersistentFlagRequired("alias"); err != nil {
		panic(err)
	}
	oneShotRunCmd.PersistentFlags().StringVar(&flagSpecletPath, "speclet-path", "", "path to the clique speclet")
	if err := oneShotRunCmd.MarkPersistentFlagRequired("speclet-path"); err != nil {
		panic(err)
	}
	oneShotRunCmd.PersistentFlags().StringVar(&flagFamily, "family", "chyt", "strawberry family")
	rootCmd.AddCommand(oneShotRunCmd)
}

func doOneShotRun() error {
	var config app.OneShotRunnerConfig
	loadConfig(flagConfigPath, &config)
	config.StrawberryRoot = getStrawberryRoot(config.StrawberryRoot)
	options := app.Options{
		LogToStderr: flagLogToStderr,
	}
	specletYson := readConfig(flagSpecletPath)
	var ctor func(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, rawConfig yson.RawValue) strawberry.Controller
	if flagFamily == "chyt" {
		ctor = chyt.NewController
	} else if flagFamily == "jupyt" {
		ctor = jupyt.NewController
	} else if flagFamily == "tryt" {
		ctor = tryt.NewController
	} else {
		panic(fmt.Errorf("unknown strawberry family %v", flagFamily))
	}
	runner := app.NewOneShotRunner(&config, &options, strawberry.ControllerFactory{Ctor: ctor, Config: config.Controller})
	return runner.Run(flagCliqueAlias, specletYson)
}
