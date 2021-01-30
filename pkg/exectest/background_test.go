package exectest

import (
	"os/exec"
	"testing"
)

func TestBackground(t *testing.T) {
	cmd := exec.Command("ping", "-c3", "8.8.8.8")
	bg := NewBackground(t, cmd)
	defer bg.Close()
	bg.Name = "ping"
	bg.LogStdout = true
	bg.Start()
	<-bg.Done()
}
