// Package subprocesstest helps running subprocesses as part of tests.
//
// Check the test files of this package for examples.
package exectest

import (
	"bytes"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// Background is background command being run.
type Background struct {
	tb      testing.TB
	Cmd     *exec.Cmd
	wg      sync.WaitGroup
	done    chan struct{}
	err     error
	errLock sync.Mutex
	// Log command output to tests.
	Name      string
	LogStdout bool
	LogStderr bool
}

// NewBackground prepares a command to run in the background of a test.
func NewBackground(tb testing.TB, cmd *exec.Cmd) *Background {
	return &Background{
		tb:   tb,
		Cmd:  cmd,
		done: make(chan struct{}, 1),
	}
}

// Start spawns a goroutine running the process in the background.
// After calling Start, accessing the provided exec.Cmd is unsafe until Close() returns.
// Can only be called once.
func (b *Background) Start() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(b.done)
		var prefix string
		if b.Name != "" {
			prefix = b.Name + ": "
		}
		if b.LogStdout {
			b.Cmd.Stdout = &PipeCapture{
				Prefix: prefix,
				TB:     b.tb,
			}
		}
		if b.LogStderr {
			b.Cmd.Stderr = &PipeCapture{
				Prefix: prefix,
				TB:     b.tb,
			}
		}
		err := b.Cmd.Run()
		b.errLock.Lock()
		b.err = err
		b.errLock.Unlock()
	}()
}

// Close must be called before the test context completes,
// regardless whether the command exited successfully.
// Close is idempotent.
func (b *Background) Close() {
	if b.Cmd.Process != nil {
		_ = b.Cmd.Process.Kill()
	}
	b.wg.Wait()
}

// Done returns a channel that closes when the command exits.
func (b *Background) Done() <-chan struct{} {
	return b.done
}

// Err returns any error that occurred with the process.
func (b *Background) Err() error {
	b.errLock.Lock()
	defer b.errLock.Unlock()
	return b.err
}

type PipeCapture struct {
	TB     testing.TB
	Prefix string
	buf    bytes.Buffer
}

func (w *PipeCapture) Write(buf []byte) (n int, err error) {
	splits := bytes.Split(buf, []byte("\n"))
	if len(splits) <= 1 {
		w.buf.Write(buf)
	} else {
		w.buf.Write(splits[0])
		w.line(string(splits[0]))
		w.buf.Reset()
		for i := 1; i < len(splits)-1; i++ {
			w.line(string(splits[i]))
		}
		w.buf.Write(splits[len(splits)-1])
	}
	return len(buf), nil
}

func (w *PipeCapture) Flush() {
	buf := w.buf.String()
	lines := strings.Split(buf, "\n")
	for _, line := range lines {
		if len(line) > 0 {
			w.line(line)
		}
	}
	w.buf.Reset()
}

func (w *PipeCapture) line(s string) {
	w.TB.Log(w.Prefix + s)
}
