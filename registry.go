package toolruntime

import (
	"fmt"
	"sort"
	"sync"
)

// Registry 保存工具定义及其版本，受理时绑定当时生效的版本。
type Registry struct {
	mu      sync.RWMutex
	defs    map[string]map[string]*ToolDefinition // name -> version -> def
	current map[string]string                     // name -> 当前版本
}

func NewRegistry() *Registry {
	return &Registry{
		defs:    make(map[string]map[string]*ToolDefinition),
		current: make(map[string]string),
	}
}

// Register 登记一个工具定义；同名工具的新版本不影响已受理的调用。
func (r *Registry) Register(def *ToolDefinition) error {
	if def == nil {
		return fmt.Errorf("definition is nil")
	}
	if def.Name == "" {
		return fmt.Errorf("tool name is required")
	}
	if def.Version == "" {
		return fmt.Errorf("tool version is required")
	}
	if def.Params == nil {
		return fmt.Errorf("tool params schema is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.defs[def.Name]; !ok {
		r.defs[def.Name] = make(map[string]*ToolDefinition)
	}
	if _, exists := r.defs[def.Name][def.Version]; exists {
		return fmt.Errorf("tool %q version %q already registered", def.Name, def.Version)
	}
	// 深拷贝，避免调用方后续改动影响已登记定义
	cp := *def
	if def.Params != nil {
		schemaCopy := *def.Params
		schemaCopy.Properties = cloneSchemas(def.Params.Properties)
		schemaCopy.Items = cloneSchema(def.Params.Items)
		if def.Params.RequiredFields != nil {
			schemaCopy.RequiredFields = append([]string(nil), def.Params.RequiredFields...)
		}
		if def.Params.Enum != nil {
			schemaCopy.Enum = append([]any(nil), def.Params.Enum...)
		}
		cp.Params = &schemaCopy
	}
	r.defs[def.Name][def.Version] = &cp
	r.current[def.Name] = def.Version
	return nil
}

func cloneSchema(s *Schema) *Schema {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Properties = cloneSchemas(s.Properties)
	cp.Items = cloneSchema(s.Items)
	if s.RequiredFields != nil {
		cp.RequiredFields = append([]string(nil), s.RequiredFields...)
	}
	if s.Enum != nil {
		cp.Enum = append([]any(nil), s.Enum...)
	}
	return &cp
}

func cloneSchemas(m map[string]*Schema) map[string]*Schema {
	if m == nil {
		return nil
	}
	out := make(map[string]*Schema, len(m))
	for k, v := range m {
		out[k] = cloneSchema(v)
	}
	return out
}

// Current 返回某工具当前生效的定义。
func (r *Registry) Current(name string) (*ToolDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ver, ok := r.current[name]
	if !ok {
		return nil, false
	}
	def, ok := r.defs[name][ver]
	return def, ok
}

// Version 返回某工具指定版本的定义。
func (r *Registry) Version(name, version string) (*ToolDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, ok := r.defs[name]
	if !ok {
		return nil, false
	}
	def, ok := versions[version]
	return def, ok
}

// Versions 返回某工具的所有版本号（升序）。
func (r *Registry) Versions(name string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.defs[name]
	out := make([]string, 0, len(versions))
	for v := range versions {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Names 返回所有已登记工具名。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.defs))
	for n := range r.defs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
