package rbd

import (
	"sync"
	"fmt"
	"os/exec"
	"time"
	"context"

	"github.com/contiv/executor"
)

var(
	engineInitFnMap = map[string]EngineInitFn{}
	engineInitFnLock = &sync.RWMutex{}
)

func runWithTimeout(cmd *exec.Cmd, timeout time.Duration) (*executor.ExecResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return executor.NewCapture(cmd).Run(ctx)
}

type RBDEngine interface {
	Create(id string, megaSize int, ignoreExist bool) (string, bool, error)
	Remove(id string) error
	Clone(id, parent string, ignoreExist bool, size int) (string, bool, error)
	Map(id string) (string, error)
	Unmap(id string) error
//	Init(home string, opts []string) error
	MountPoint(id string) string
	Name() string
	Params() []string
	Mapped(id string) (bool, string, error)
}

type EngineInitFn func(home string, opts []string) (RBDEngine, error)

func RegisterRBDEngine(driver string, fn EngineInitFn) error {
	engineInitFnLock.Lock()
	defer engineInitFnLock.Unlock()

	_,ok := engineInitFnMap[driver]
	if ok {
		return fmt.Errorf("rbd engine %s has been register", driver)
	}

	engineInitFnMap[driver] = fn
	return nil
}

func NewRBDEngine(driver string, home string, opts []string) (RBDEngine, error) {
	engineInitFnLock.RLock()
	defer engineInitFnLock.RUnlock()

	fn,ok := engineInitFnMap[driver]
	if !ok {
		return nil, fmt.Errorf("rbd engine %s has not been register", driver)
	}

	return fn(home, opts)
}