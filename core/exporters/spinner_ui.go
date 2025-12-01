package exporters

import (
	"github.com/yarlson/pin"
)

type spinner struct {
	p *pin.Pin
}

func newSpinner() spinner {
	return spinner{p: pin.New("Exporting rows",
		pin.WithSpinnerColor(pin.ColorCyan),
		pin.WithTextColor(pin.ColorYellow)),
	}
}

func (s spinner) showProgressSpinner(message string) {
	if s.p == nil {
		return
	}
	s.p.UpdateMessage(message)
}

func (s spinner) stopSpinner(message string) {
	if s.p == nil {
		return
	}
	s.p.Stop(message)
}
