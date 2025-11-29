package ui

import (
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
)

func NewProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("Exporting rows"),
		progressbar.OptionEnableColorCodes(false),
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionShowBytes(false),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionSpinnerType(14), // style simple et discret
		progressbar.OptionSetWidth(15),
	)
}
