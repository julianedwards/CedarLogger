package encode

import "sync"

var globalRegistry = &encodingRegistry{
	registry: map[string]Encoding{
		TEXT: &textEncoding{},
		JSON: &jsonEncoding{},
	},
}

func GetGlobalRegistry() *encodingRegistry { return globalRegistry }

type encodingRegistry struct {
	mu       sync.RWMutex
	registry map[string]Encoding
}

func NewEncodingRegistry() *encodingRegistry {
	return &encodingRegistry{
		registry: map[string]Encoding{},
	}
}

func (r *encodingRegistry) AddNew(encoding Encoding) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.registry[encoding.String()]; ok {
		return
	}

	r.registry[encoding.String()] = encoding
}

func (r *encodingRegistry) Get(name string) (Encoding, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	encoding, ok := r.registry[name]
	return encoding, ok
}
