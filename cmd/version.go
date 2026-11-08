package cmd

import (
	"fmt"

	"github.com/fbz-tec/pgxport/v2/internal/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version.GetInfo())
	},
}
