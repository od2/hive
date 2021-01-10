package providers

import (
	"fmt"
	"net"
	"os"
)

func ListenUnix(path string) (net.Listener, error) {
	stat, statErr := os.Stat(path)
	if os.IsNotExist(statErr) {
		return net.Listen("unix", path)
	} else if statErr != nil {
		return nil, statErr
	}
	// Socket still exists, clean up.
	if stat.Mode()|os.ModeSocket == 0 {
		return nil, fmt.Errorf("existing file is not a socket: %s", path)
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("failed to remove socket: %w", err)
	}
	return net.Listen("unix", path)
}
