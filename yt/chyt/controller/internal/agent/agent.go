package agent

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Agent struct {
	ytc        yt.Client
	l          log.Logger
	controller strawberry.Controller
	config     *Config

	aliasToOp map[string]*strawberry.Oplet

	hostname string
	proxy    string
	family   string
	root     ypath.Path

	// nodeCh receives events of form "particular node in root has changed revision".
	nodeCh <-chan PathsOrError

	// runningOpsCh periodically receives all running vanilla operations
	// with operation namespace from controller.
	runningOpsCh <-chan OperationsOrError

	started   bool
	ctx       context.Context
	cancelCtx context.CancelFunc

	backgroundStopCh chan struct{}
	healthState      *agentHealthState
}

func NewAgent(proxy string, ytc yt.Client, l log.Logger, controller strawberry.Controller, config *Config) *Agent {
	hostname, err := os.Hostname()
	if err != nil {
		l.Fatal("error getting hostname", log.Error(err))
	}

	tf := config.HealthCheckerToleranceFactorOrDefault()
	return &Agent{
		ytc:              ytc,
		l:                l,
		controller:       controller,
		config:           config,
		hostname:         hostname,
		family:           controller.Family(),
		root:             controller.Root(),
		proxy:            proxy,
		backgroundStopCh: make(chan struct{}),
		healthState: newAgentHealthState(
			time.Duration(tf*float64(config.PassPeriodOrDefault())),
			time.Duration(tf*float64(config.RevisionCollectPeriodOrDefault())),
			time.Duration(tf*float64(config.CollectOperationsPeriodOrDefault()))),
	}
}

func (a *Agent) updateACLs() error {
	var result []struct {
		Alias string   `yson:",value"`
		ACL   []yt.ACE `yson:"principal_acl,attr"`
	}

	err := a.ytc.ListNode(a.ctx,
		strawberry.AccessControlNamespacesPath.Child(a.controller.Family()),
		&result,
		&yt.ListNodeOptions{Attributes: []string{"principal_acl"}})

	if err != nil {
		return err
	}

	aclUpdated := make(map[string]bool)

	for _, node := range result {
		aclUpdated[node.Alias] = true
		if oplet, ok := a.aliasToOp[node.Alias]; ok {
			oplet.SetACL(node.ACL)
		}
	}

	for alias, oplet := range a.aliasToOp {
		if !aclUpdated[alias] {
			a.l.Error("oplet is broken: missing acl node", log.String("alias", alias))
			a.unregisterOplet(oplet)
		}
	}

	return nil
}

func (a *Agent) processRunningOperations(runningOps []yt.OperationStatus) error {
	family := a.controller.Family()
	l := log.With(a.l, log.String("family", family))

	startedAt := time.Now()

	l.Info("processing running operations")
	toAbort := make([]yt.OperationID, 0)
	foundAliases := make(map[string]bool)
	for _, op := range runningOps {
		if op.BriefSpec == nil {
			// This may happen on early stages of operation lifetime.
			continue
		}

		opAlias, ok := op.BriefSpec["alias"]
		if !ok {
			l.Debug("operation misses alias (how is that possible?), aborting it",
				log.String("operation_id", op.ID.String()))
			toAbort = append(toAbort, op.ID)
			continue
		}
		alias := opAlias.(string)[1:]

		oplet, ok := a.aliasToOp[alias]
		if !ok {
			l.Debug("operation alias unknown, aborting it",
				log.String("alias", alias), log.String("operation_id", op.ID.String()))
			toAbort = append(toAbort, op.ID)
			continue
		}
		foundAliases[alias] = true

		opID, opState := oplet.OperationInfo()
		if !oplet.UpToDateWithCypress() {
			continue
		}

		if opID != op.ID {
			l.Debug("yt operation has unexpected id, aborting it",
				log.String("alias", alias), log.String("operation_id", op.ID.String()),
				log.String("expected_id", opID.String()))
			toAbort = append(toAbort, op.ID)
			continue
		}

		if opState != op.State {
			oplet.UpdateOpStatus(&op)
		}
	}

	abortCh := make(chan yt.OperationID, len(toAbort))
	checkCh := make(chan *strawberry.Oplet, len(a.aliasToOp)-len(foundAliases))

	workerNumber := a.config.PassWorkerNumberOrDefault()
	var wg sync.WaitGroup
	wg.Add(workerNumber)

	for i := 0; i < workerNumber; i++ {
		go func() {
			defer wg.Done()

			// Abort dangling operations. This results in filtering those
			// which are not listed in our aliasToOp.
			for opID := range abortCh {
				err := a.ytc.AbortOperation(a.ctx, opID, nil)
				if err != nil {
					l.Error("error aborting operation",
						log.String("operation_id", opID.String()),
						log.Error(err))
				}
			}

			// Additionally check operation liveness for all oplets that have no running operations
			// but have info about operation in persistent state.
			for oplet := range checkCh {
				_ = oplet.CheckOperationLiveness(a.ctx)
			}
		}()
	}

	for _, opID := range toAbort {
		abortCh <- opID
	}
	close(abortCh)

	checkedCnt := 0
	for alias, oplet := range a.aliasToOp {
		wasProcessed := foundAliases[alias]
		if wasProcessed || !oplet.UpToDateWithCypress() || !oplet.HasYTOperation() {
			continue
		}
		checkCh <- oplet
		checkedCnt++
	}
	close(checkCh)

	wg.Wait()

	l.Info("finished processing running operations",
		log.Duration("elapsed_time", time.Since(startedAt)),
		log.Int("total_operations_count", len(runningOps)),
		log.Int("explicit_checked_count", checkedCnt),
		log.Int("aborted_operations_count", len(toAbort)))

	return nil
}

func (a *Agent) processOplets() {
	startedAt := time.Now()

	a.l.Info("starting processing oplets")

	workerNumber := a.config.PassWorkerNumberOrDefault()
	var wg sync.WaitGroup
	wg.Add(workerNumber)

	opletsChan := make(chan *strawberry.Oplet, len(a.aliasToOp))
	workerPassDur := make([]time.Duration, workerNumber)
	workerMaxPassDur := make([]struct {
		dur   time.Duration
		alias string
	}, workerNumber)
	for i := 0; i < workerNumber; i++ {
		go func(idx int) {
			defer wg.Done()
			for oplet := range opletsChan {
				start := time.Now()

				// We don't need to check the liveness of the operation in the agent,
				// since we do it while processing the result of the operation listing.
				_ = oplet.Pass(a.ctx, false /*checkOpLiveness*/)

				passDur := time.Since(start)
				workerPassDur[idx] += passDur
				if passDur > workerMaxPassDur[idx].dur {
					workerMaxPassDur[idx].dur = passDur
					workerMaxPassDur[idx].alias = oplet.Alias()
				}
			}
		}(i)
	}

	for _, oplet := range a.aliasToOp {
		opletsChan <- oplet
	}
	close(opletsChan)

	wg.Wait()

	logFields := make([]log.Field, 0, 5)
	logFields = append(logFields, log.Duration("elapsed_time", time.Since(startedAt)))
	var totalPassDur time.Duration
	var maxPassDur time.Duration
	var maxPassAlias string
	for i := 0; i < workerNumber; i++ {
		totalPassDur += workerPassDur[i]
		if workerMaxPassDur[i].dur > maxPassDur {
			maxPassDur = workerMaxPassDur[i].dur
			maxPassAlias = workerMaxPassDur[i].alias
		}
	}
	var avgPassTime time.Duration
	var avgWorkerTime time.Duration
	if len(a.aliasToOp) > 0 {
		avgPassTime = totalPassDur / time.Duration(len(a.aliasToOp))
	}
	if workerNumber > 0 {
		avgWorkerTime = totalPassDur / time.Duration(workerNumber)
	}
	logFields = append(logFields, log.Duration("max_pass_time", maxPassDur), log.String("max_pass_alias", maxPassAlias))
	logFields = append(logFields, log.Duration("avg_pass_time", avgPassTime))
	logFields = append(logFields, log.Duration("avg_worker_time", avgWorkerTime))

	a.l.Info("finished processing oplets", logFields...)
}

func (a *Agent) pass() {
	startedAt := time.Now()

	a.l.Info("starting pass", log.Int("oplet_count", len(a.aliasToOp)))

	if err := a.updateControllerState(); err != nil {
		a.healthState.SetPassState(yterrors.Err("failed to update controller's state", err))
		return
	}

	if err := a.updateACLs(); err != nil {
		a.healthState.SetPassState(yterrors.Err("failed to update ACLs", err))
		return
	}

	a.processOplets()
	for _, oplet := range a.aliasToOp {
		if oplet.Broken() {
			a.l.Info("unregistering oplet: it is broken",
				log.String("alias", oplet.Alias()),
				log.String("reason", oplet.BrokenReason()))
			a.unregisterOplet(oplet)
		} else if oplet.Inappropriate() {
			a.l.Info("unregistering oplet: it is inappropriate", log.String("alias", oplet.Alias()))
			a.unregisterOplet(oplet)
		}
	}

	// Sanity check.
	for alias, oplet := range a.aliasToOp {
		if oplet.Alias() != alias {
			panic(fmt.Errorf("invariant violation: alias %v points to oplet for operation with alias %v", alias, oplet.Alias()))
		}
	}

	a.l.Info("pass completed", log.Duration("elapsed_time", time.Since(startedAt)))
	a.healthState.SetPassState(nil)
}

func (a *Agent) updateControllerState() error {
	changed, err := a.controller.UpdateState()
	if err != nil {
		return err
	}
	if changed {
		for _, oplet := range a.aliasToOp {
			oplet.SetPendingRestart("controller's state changed")
		}
	}
	return nil
}

func (a *Agent) initializeFromCypress() error {
	a.l.Info("initializing agent from cypress")

	_, err := a.controller.UpdateState()
	if err != nil {
		a.l.Error("error occured during updating controller state", log.Error(err))
		a.healthState.SetInitState(yterrors.Err("failed to update controller state", err))
		return err
	}

	var initialAliases []string
	err = a.ytc.ListNode(a.ctx, a.root, &initialAliases, nil)
	if err != nil {
		a.l.Error("error occured during initializing agent", log.Error(err))
		a.healthState.SetInitState(yterrors.Err("failed to list aliases", err))
		return err
	}

	// TODO(dakovalkov): we can do initialization more optimal with get on the whole directory.
	for _, alias := range initialAliases {
		a.registerNewOplet(alias)
	}

	a.l.Info("agent initialized", log.Int("oplet_count", len(initialAliases)))
	a.healthState.SetInitState(nil)
	return nil
}

func (a *Agent) background() {
	passPeriod := time.Duration(a.config.PassPeriodOrDefault())
	a.l.Info("starting background activity", log.Duration("period", passPeriod))
	ticker := time.NewTicker(passPeriod)

	defer func() {
		a.l.Info("background activity stopped")
		a.backgroundStopCh <- struct{}{}
	}()

	// The agent initialization is done in the background because it may fail
	// (e.g. the cluster is unavailable) and we want to retry it till we finally
	// initialize it.
	for a.initializeFromCypress() != nil {
		// Back off before the next try.
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		case event := <-a.nodeCh:
			if event.Error != nil {
				a.healthState.SetTrackNodesState(event.Error)
				continue
			}
			for _, path := range event.Paths {
				tokens := tokenize(path)
				if len(tokens) >= 1 {
					alias := tokens[0]
					subnodes := tokens[1:]
					oplet, ok := a.aliasToOp[alias]
					switch {
					case reflect.DeepEqual(subnodes, []string{}) ||
						reflect.DeepEqual(subnodes, []string{"speclet"}) ||
						reflect.DeepEqual(subnodes, []string{"secrets"}):
						if ok {
							oplet.OnCypressNodeChanged()
						} else {
							a.registerNewOplet(alias)
						}
					}
				}
			}
			a.healthState.SetTrackNodesState(nil)
		case event := <-a.runningOpsCh:
			if event.Error != nil {
				a.healthState.SetTrackOpsState(event.Error)
				continue
			}

			if err := a.processRunningOperations(event.Operations); err != nil {
				err = yterrors.Err("failed to process running operations", err)
				a.healthState.SetTrackOpsState(err)
				continue
			}
			a.healthState.SetTrackOpsState(nil)
		case <-ticker.C:
			a.pass()
		}
	}
}

func (a *Agent) GetAgentInfo() strawberry.AgentInfo {
	clusterURLTemplateData := struct {
		Proxy string
	}{
		a.proxy,
	}

	return strawberry.AgentInfo{
		StrawberryRoot:        a.root,
		Hostname:              a.hostname,
		Stage:                 a.config.Stage,
		Proxy:                 a.proxy,
		Family:                a.family,
		OperationNamespace:    a.OperationNamespace(),
		RobotUsername:         a.config.RobotUsername,
		DefaultNetworkProject: a.config.DefaultNetworkProject,
		ClusterURL:            strawberry.ExecuteTemplate(a.config.ClusterURLTemplate, clusterURLTemplateData),
	}
}

func (a *Agent) getOpletOptions(alias string) strawberry.OpletOptions {
	return strawberry.OpletOptions{
		AgentInfo:    a.GetAgentInfo(),
		Alias:        alias,
		Controller:   a.controller,
		Logger:       a.l,
		UserClient:   a.ytc,
		SystemClient: a.ytc,
	}
}

func (a *Agent) registerOplet(oplet *strawberry.Oplet) {
	if _, ok := a.aliasToOp[oplet.Alias()]; ok {
		panic(fmt.Errorf("invariant violation: alias %v is already registered", oplet.Alias()))
	}
	a.aliasToOp[oplet.Alias()] = oplet

	a.l.Info("oplet registered", log.String("alias", oplet.Alias()))
}

func (a *Agent) registerNewOplet(alias string) {
	a.registerOplet(strawberry.NewOplet(a.getOpletOptions(alias)))
}

func (a *Agent) unregisterOplet(oplet *strawberry.Oplet) {
	if actual := a.aliasToOp[oplet.Alias()]; actual != oplet {
		panic(fmt.Errorf("invariant violation: alias %v expected to match oplet %v, actual %v",
			oplet.Alias(), oplet, actual))
	}
	delete(a.aliasToOp, oplet.Alias())
	a.l.Info("oplet unregistered", log.String("alias", oplet.Alias()))
}

func (a *Agent) Start() {
	if a.started {
		return
	}

	a.l.Info("starting agent")
	a.started = true
	a.ctx, a.cancelCtx = context.WithCancel(context.Background())

	a.aliasToOp = make(map[string]*strawberry.Oplet)

	revisionCollectPeriod := time.Duration(a.config.RevisionCollectPeriodOrDefault())
	a.nodeCh = TrackChildren(a.ctx, a.root, revisionCollectPeriod, a.ytc, a.l)

	a.runningOpsCh = CollectOperations(
		a.ctx,
		a.ytc,
		time.Duration(a.config.CollectOperationsPeriodOrDefault()),
		a.OperationNamespace(),
		a.l)

	go a.background()
	a.l.Info("agent started")
}

func (a *Agent) Stop() {
	if !a.started {
		return
	}
	a.l.Info("stopping agent")
	a.cancelCtx()
	<-a.backgroundStopCh

	a.ctx = nil
	a.aliasToOp = nil
	a.nodeCh = nil
	a.runningOpsCh = nil
	a.started = false
	a.l.Info("agent stopped")
}

// OperationNamespace generates a special value unique across controllers
// which allows to mark and effectively filter its operations.
func (a *Agent) OperationNamespace() string {
	return a.controller.Family() + ":" + a.config.Stage
}

func (a *Agent) CheckHealth() error {
	return a.healthState.Get()
}
