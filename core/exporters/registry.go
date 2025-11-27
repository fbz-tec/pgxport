package exporters

import (
	"fmt"
	"sort"
	"strings"
)

type Factory func() Exporter

var registry = map[string]Factory{}

func Register(format string, factory Factory) error {
	format = strings.ToLower(strings.TrimSpace(format))
	if _, exists := registry[format]; exists {
		return fmt.Errorf("exporter: format %q already registered", format)
	}
	registry[format] = factory
	return nil
}

func Get(format string) (Exporter, error) {
	factory, ok := registry[format]
	if !ok {
		return nil, fmt.Errorf("unsupported format: %q (available: %s)",
			format, strings.Join(List(), ", "))
	}
	return factory(), nil
}

func List() []string {
	formats := make([]string, 0, len(registry))
	for name := range registry {
		formats = append(formats, name)
	}
	sort.Strings(formats)
	return formats
}

func MustRegister(format string, factory Factory) {
	if err := Register(format, factory); err != nil {
		panic(err)
	}
}
