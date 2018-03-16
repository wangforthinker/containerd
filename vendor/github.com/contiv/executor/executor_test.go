package executor

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	. "testing"
	"time"

	"github.com/Sirupsen/logrus"

	"golang.org/x/net/context"

	. "gopkg.in/check.v1"
)

type execSuite struct{}

var cmd = []string{"/bin/sleep", "200000000"}

var _ = Suite(&execSuite{})

func TestExec(t *T) {
	logrus.SetLevel(logrus.DebugLevel)
	TestingT(t)
}

func (es *execSuite) TestProperties(c *C) {
	mycmd := exec.Command(cmd[0], cmd[1:]...)
	e := New(mycmd)
	c.Assert(e, NotNil)
	c.Assert(mycmd, DeepEquals, e.command)
	c.Assert(e.LogInterval, Equals, 1*time.Minute)
	c.Assert(e.Stdin, IsNil)
}

func (es *execSuite) TestStartWait(c *C) {
	e := New(exec.Command(cmd[0], cmd[1:]...))
	c.Assert(e, NotNil)
	c.Assert(e.Start(), IsNil)
	c.Assert(e.PID(), Not(Equals), uint32(0))
	c.Assert(e.command.Process, NotNil)
	// the sleep requires we signal the process since it'll sleepp forever.
	c.Assert(e.command.Process.Signal(syscall.SIGTERM), IsNil)
	er, err := e.Wait(context.Background())
	c.Assert(err, NotNil)
	c.Assert(er, NotNil)
	c.Assert(er.ExitStatus, Equals, 0)
	c.Assert(er.Runtime, Not(Equals), time.Duration(0))
}

func (es *execSuite) TestStdio(c *C) {
	cmd := exec.Command("/bin/echo", "yes")
	e := NewCapture(cmd)
	c.Assert(e.Start(), IsNil)
	er, err := e.Wait(context.Background())
	c.Assert(err, IsNil)
	c.Assert(er, NotNil)
	c.Assert(er.Stdout, Equals, "yes\n")
	cmd = exec.Command("sh", "-c", "echo yes 1>&2")
	e = NewCapture(cmd)
	c.Assert(e.Start(), IsNil)
	er, _ = e.Wait(context.Background())
	c.Assert(er, NotNil)
	c.Assert(er.Stderr, Equals, "yes\n")

	cmd = exec.Command("/bin/echo", "yes")
	e = NewIO(cmd)
	c.Assert(e.Start(), IsNil)

	buf := new(bytes.Buffer)
	io.Copy(buf, e.Out())
	c.Assert(buf.String(), Equals, "yes\n")

	cmd = exec.Command("sh", "-c", "echo yes 1>&2")
	e = NewIO(cmd)
	c.Assert(e.Start(), IsNil)

	buf = new(bytes.Buffer)
	io.Copy(buf, e.Err())
	c.Assert(buf.String(), Equals, "yes\n")

	cmd = exec.Command("cat")
	e = NewCapture(cmd)
	e.Stdin = bytes.NewBufferString("foo")
	c.Assert(e.Start(), IsNil)
	er, err = e.Wait(context.Background())
	c.Assert(err, IsNil)
	c.Assert(er.Stdout, Equals, "foo")

	cmd = exec.Command("sh", "-c", "for i in $(seq 0 1000); do echo $i; done")
	e = NewCapture(cmd)
	c.Assert(e.Start(), IsNil)
	er, err = e.Wait(context.Background())
	c.Assert(err, IsNil)
	c.Assert(len(strings.Split(er.Stdout, "\n")), Equals, 1002)
}

func (es *execSuite) TestTimeout(c *C) {
	e := New(exec.Command(cmd[0], cmd[1:]...))
	results := []string{}
	loggerFunc := func(s string, args ...interface{}) {
		results = append(results, s)
	}

	e.LogFunc = loggerFunc
	c.Assert(e.Start(), IsNil)
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	er, err := e.Wait(ctx)
	c.Assert(results[0], Equals, "Command %v terminated due to timeout or cancellation. It may not have finished!")
	c.Assert(err, Equals, context.DeadlineExceeded)

	e = New(exec.Command("/bin/sleep", "2"))
	e.LogFunc = logrus.Infof
	results = []string{}
	ctx, _ = context.WithTimeout(context.Background(), 4*time.Second)
	er, err = e.Run(ctx)
	c.Assert(err, IsNil)
	c.Assert(er.ExitStatus, Equals, 0)
	time.Sleep(2 * time.Second)
	c.Assert(len(results), Equals, 0)
}

func (es *execSuite) TestLogger(c *C) {
	results := []string{}
	loggerFunc := func(s string, args ...interface{}) {
		results = append(results, s)
	}

	cmd := exec.Command("/bin/sleep", "2")

	e := New(cmd)
	e.LogFunc = loggerFunc
	e.LogInterval = 1 * time.Second
	c.Assert(e.Start(), IsNil)
	time.Sleep(2 * time.Second)
	er, _ := e.Wait(context.Background())
	c.Assert(er.ExitStatus, Equals, 0)
	c.Assert(er.Runtime > 2*time.Second, Equals, true)
	// sometimes (depending on how long it takes to `cmd.Start()` launch) the
	// logger will log twice and other times it will only log once. To keep the
	// tests reliable, we only test the first log.
	c.Assert(results[0], Equals, "%v has been running for %v")
}

func (es *execSuite) TestLoggerZeroOrNegativeLogInterval(c *C) {
	results := []string{}
	loggerFunc := func(s string, args ...interface{}) {
		results = append(results, s)
	}

	cmd := exec.Command("/bin/sleep", "2")
	e := New(cmd)
	e.LogFunc = loggerFunc
	e.LogInterval = 0
	c.Assert(e.Start(), IsNil)
	time.Sleep(2 * time.Second)
	er, _ := e.Wait(context.Background())
	c.Assert(er.ExitStatus, Equals, 0)
	c.Assert(er.Runtime > 2*time.Second, Equals, true)
	c.Assert(len(results), Equals, 0)

	cmd = exec.Command("/bin/sleep", "2")
	e = New(cmd)
	e.LogFunc = loggerFunc
	e.LogInterval = -1
	c.Assert(e.Start(), IsNil)
	time.Sleep(2 * time.Second)
	er, _ = e.Wait(context.Background())
	c.Assert(er.ExitStatus, Equals, 0)
	c.Assert(er.Runtime > 2*time.Second, Equals, true)
	c.Assert(len(results), Equals, 0)
}

func (es *execSuite) TestString(c *C) {
	e := New(exec.Command(cmd[0], cmd[1:]...))
	c.Assert(e.String(), Equals, "[/bin/sleep 200000000] (/bin/sleep) (pid: 0)")
	c.Assert(e.Start(), IsNil)
	c.Assert(e.PID(), Not(Equals), uint32(0))
	c.Assert(e.String(), Equals, fmt.Sprintf("[/bin/sleep 200000000] (/bin/sleep) (pid: %v)", e.PID()))
	c.Assert(e.command.Process.Signal(syscall.SIGTERM), IsNil)
	er, _ := e.Wait(context.Background())
	c.Assert(er, NotNil)
	c.Assert(er.ExitStatus, Equals, 0)
}

func (es *execSuite) TestCancel(c *C) {
	e := New(exec.Command(cmd[0], cmd[1:]...))
	results := []string{}
	loggerFunc := func(s string, args ...interface{}) {
		results = append(results, s)
	}

	e.LogFunc = loggerFunc
	c.Assert(e.Start(), IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	erChan := make(chan *ExecResult, 1)
	errChan := make(chan error, 1)
	go func() {
		er, err := e.Wait(ctx)
		erChan <- er
		errChan <- err
	}()

	cancel()
	er := <-erChan
	err := <-errChan
	c.Assert(er, NotNil)
	c.Assert(er.executor, NotNil)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, context.Canceled)
	c.Assert(results[0], Equals, "Command %v terminated due to timeout or cancellation. It may not have finished!")
}

func (es *execSuite) TestError(c *C) {
	e := New(exec.Command("sh", "-c", "exit 2"))
	er, err := e.Run(context.Background())
	c.Assert(er, NotNil)
	c.Assert(err, NotNil)
	c.Assert(er.ExitStatus, Equals, 2)
	_, ok := err.(*exec.ExitError)
	c.Assert(ok, Equals, true)

	e = New(exec.Command("sh", "-c", "exit 28"))
	er, err = e.Run(context.Background())
	c.Assert(er, NotNil)
	c.Assert(err, NotNil)
	c.Assert(er.ExitStatus, Equals, 28)
	_, ok = err.(*exec.ExitError)
	c.Assert(ok, Equals, true)
}
