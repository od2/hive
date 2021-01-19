package admin_tool

import "github.com/spf13/cobra"

// Cmd is the admin-tool sub-command.
var Cmd = cobra.Command{
	Use:   "admin-tool",
	Short: "Debug utility for administering hive components",
}
