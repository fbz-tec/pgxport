package ui

import (
	"context"

	"github.com/yarlson/pin"
)

type Spinner struct {
	p      *pin.Pin
	cancel context.CancelFunc
}

func NewSpinner() *Spinner {
	p := pin.New("Processing...",
		pin.WithSpinnerColor(pin.ColorCyan),
		pin.WithTextColor(pin.ColorYellow))
	return &Spinner{p: p}
}

func (s *Spinner) Start() {
	if s == nil || s.p == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.p.Start(ctx)
}

func (s *Spinner) Update(message string) {
	if s == nil || s.p == nil {
		return
	}
	s.p.UpdateMessage(message)
}

func (s *Spinner) Stop(message string) {
	if s == nil || s.p == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.p.Stop(message)
}
