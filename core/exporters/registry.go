package exporters

import (
	"fmt"
	"sort"
	"strings"
)

// Factory is a function type that creates a new Exporter instance.
type Factory func() Exporter

var registry = map[string]Factory{}

// Register registers a new exporter format with its factory function.
// Returns an error if the format is already registered.
func Register(format string, factory Factory) error {
	format = strings.ToLower(strings.TrimSpace(format))
	if _, exists := registry[format]; exists {
		return fmt.Errorf("exporter: format %q already registered", format)
	}
	registry[format] = factory
	return nil
}

// Get retrieves an exporter instance for the specified format.
// Returns an error if the format is not registered.
func Get(format string) (Exporter, error) {
	factory, ok := registry[format]
	if !ok {
		return nil, fmt.Errorf("unsupported format: %q (available: %s)",
			format, strings.Join(List(), ", "))
	}
	return factory(), nil
}

// List returns all registered export formats in alphabetical order.
func List() []string {
	formats := make([]string, 0, len(registry))
	for name := range registry {
		formats = append(formats, name)
	}
	sort.Strings(formats)
	return formats
}

// MustRegister registers a new exporter format and panics if registration fails.
// This is useful for initialization code where registration failures should be fatal.
func MustRegister(format string, factory Factory) {
	if err := Register(format, factory); err != nil {
		panic(err)
	}
}
